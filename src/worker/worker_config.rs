//! This module defines the worker_config struct and related functions. It allows meshsim to
//! deserialize a configuration file into a worker_config object that eventually creates a worker object.
extern crate toml;
extern crate rustc_serialize;
extern crate rand;
extern crate byteorder;

use worker::{Worker, OperationMode, Write, WorkerError, DeviceRadio, SimulatedRadio};
use worker::radio::*;
use worker::protocols::*;
use std::path::Path;
use std::fs::File;
use std::iter;
use self::rustc_serialize::hex::*;
use self::rand::{Rng, StdRng};
use std::sync::{Mutex, Arc};

///Configuration pertaining to a given radio of the worker.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct RadioConfig {
    ///The broadcast groups this radio belongs to. Ignored in device mode.
    pub broadcast_groups : Vec<String>,
    ///Simulated mode only. How likely ([0-1]) are packets to reach their destination.
    pub reliability : Option<f64>,
    ///Simulated mode only. Artificial delay (in ms) introduced to the network packets of this radio.
    pub delay : Option<u32>,
    ///Name of the network interface that this radio will use.
    pub interface_name : Option<String>,
}

impl RadioConfig {
    /// Create a new default radio configuration with all fields set to a default. This it not valid for either simulated or device mode,
    /// so users should take care to modify the appropriate values.
    pub fn new() -> RadioConfig {
        RadioConfig{ broadcast_groups : vec!(String::from("group1")),
                     reliability : Some(1.0),
                     delay : Some(0),
                     interface_name : Some(String::from("wlan0")),}
    }

    /// Consuming the underlying configuration, this method produces a Box<Radio> object that can be started and used.
    pub fn create_radio(self, operation_mode : OperationMode, range : RadioTypes, work_dir : String, worker_name : String, 
                        worker_id : String, seed : u32, r : Option<Arc<Mutex<StdRng>>>) -> Arc<Radio> {
        let rng = match r {
            Some(gen) => gen,
            None => { Arc::new(Mutex::new(Worker::rng_from_seed(seed))) }
        };

        match operation_mode {
            OperationMode::Device => { 
                let iname = self.interface_name.expect("An interface name for radio_short must be provided when operating in device_mode.");
                Arc::new( DeviceRadio::new( iname, 
                                            worker_name,
                                            worker_id, 
                                            rng,
                                            range))
            },
            OperationMode::Simulated => { 
                let delay = self.delay.unwrap_or(0);
                let reliability = self.reliability.unwrap_or(1.0);
                let bg = self.broadcast_groups;
                Arc::new(SimulatedRadio::new(delay, 
                                             reliability, 
                                             bg, 
                                             work_dir,
                                             worker_id,
                                             worker_name,
                                             RadioTypes::ShortRange,
                                             rng,
                                             range))
            },
        }
    }
}
/// Configuration for a worker object. This struct encapsulates several of the properties
/// of a worker. Its use is for the external interfaces of the worker module. This facilitates
/// that an external client such as worker_cli can create WorkerConfig objects from CLI parameters
/// or configuration files and pass the configuration around, leaving the actual construction of
/// the worker object to the worker module.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct WorkerConfig {
    ///Name of the worker.
    pub worker_name : String,
    ///Directory for the worker to operate. Must have RW access to it. Operational files and 
    ///log files will be written here.
    pub work_dir : String,
    ///Random seed used for all RNG operations.
    pub random_seed : u32,
    ///Simulated or Device operation.
    pub operation_mode : OperationMode,
    /// The protocol that this Worker should run for this configuration.
    pub protocol : Protocols,
    ///NOTE: Due to the way serde_toml works, the RadioConfig fields must be kept last in the structure.
    /// This is because they are interpreted as TOML tables, and those are always placed at the end of structures.
    ///The configuration for the short-range radio of this worker.
    pub radio_short : Option<RadioConfig>,
    ///The configuration for the short-range radio of this worker.
    pub radio_long : Option<RadioConfig>,
}

impl WorkerConfig {
    ///Creates a new configuration for a Worker with default settings.
    pub fn new() -> WorkerConfig {
        WorkerConfig{worker_name : String::from("worker1"),
                     work_dir : String::from("."),
                     random_seed : 0, //The random seed itself doesn't need to be random. Also, that makes testing more difficult.
                     operation_mode : OperationMode::Simulated,
                     protocol : Protocols::TMembership,
                     radio_short : None,
                     radio_long : None,
                    }
    }

    ///Creates a new Worker object configured with the values of this configuration object.
    pub fn create_worker(self) -> Worker {
        //Create the RNG
        let mut gen = Worker::rng_from_seed(self.random_seed);
        
        //Vector of 16 bytes set to 0
        let mut key : Vec<u8>= iter::repeat(0u8).take(16).collect();
        //Fill the vector with 16 random bytes.
        gen.fill_bytes(&mut key[..]);
        let id = key.to_hex().to_string();
        //Wrap the rng in the shared-mutable-state smart pointers
        let rng = Arc::new(Mutex::new(gen));

        //Create the radios
        let sr = match self.radio_short {
            Some(sr_config) => { 
                let sr = sr_config.create_radio(self.operation_mode, RadioTypes::ShortRange, self.work_dir.clone(), 
                                                self.worker_name.clone(), id.clone(), self.random_seed, Some(Arc::clone(&rng)));
                Some(sr)
            },
            None => { None }
        };

        let lr = match self.radio_long {
            Some(lr_config) => { 
                let sr = lr_config.create_radio(self.operation_mode, RadioTypes::LongRange, self.work_dir.clone(), 
                                                self.worker_name.clone(), id.clone(), self.random_seed, Some(Arc::clone(&rng)));
                Some(sr)
            },
            None => { None }
        };

        Worker{ name : self.worker_name,
                short_radio : sr,
                long_radio : lr,
                work_dir : self.work_dir, 
                rng : Arc::clone(&rng),
                seed : self.random_seed,
                operation_mode : self.operation_mode,
                id : id,
                protocol : self.protocol }
    }

    ///Writes the current configuration object to a formatted configuration file, that can be passed to
    ///the worker_cli binary.
    pub fn write_to_file<P: AsRef<Path>>(&self, file_path: P) -> Result<(), WorkerError> {
        //Create configuration file
        let mut file = try!(File::create(&file_path));
        let data = match toml::to_string(self) {
            Ok(d) => d,
            Err(e) => return Err(WorkerError::Configuration(format!("Error writing configuration to file: {}", e)))
        };
        let _res = try!(write!(file, "{}", data));

        Ok(())
    }
}

//**** WorkerConfig unit tests ****
#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    use super::*;
    use std::env;

    //Unit test for: WorkerConfig_new
    #[test]
    fn test_workerconfig_new() {
        let config = WorkerConfig::new();
        let config_str = "WorkerConfig { worker_name: \"worker1\", work_dir: \".\", random_seed: 0, operation_mode: Simulated, protocol: TMembership, radio_short: None, radio_long: None }";

        assert_eq!(format!("{:?}", config), config_str);
    }

    //Unit test for: WorkerConfig::create_worker
    #[ignore]
    #[test]
    fn test_workerconfig_create_worker() {
        //test_workerconfig_new and test_worker_new already test the correct instantiation of WorkerConfig and Worker.
        //Just make sure thing function translates a WorkerConfig correctly into a Worker.
        let mut config = WorkerConfig::new();
        let sr = RadioConfig::new();
        let lr = RadioConfig::new();
        config.radio_short = Some(sr);
        config.radio_long = Some(lr);
        
        println!("Worker config: {:?}", &config);
        let worker = config.create_worker();
        let default_worker_display = "Worker { name: \"worker1\", id: \"416d77337e24399dc7a5aa058039f72a\", short_radio: Some(SimulatedRadio { delay: 0, reliability: 1.0, broadcast_groups: [\"group1\"], work_dir: \".\", me: Peer { id: \"416d77337e24399dc7a5aa058039f72a\", name: \"worker1\", address: \"./bcg/group1/416d77337e24399dc7a5aa058039f72a.socket\" }, range: ShortRange, rng: Mutex { data: StdRng { rng: Isaac64Rng {} } } }), long_radio: Some(SimulatedRadio { delay: 0, reliability: 1.0, broadcast_groups: [\"group1\"], work_dir: \".\", me: Peer { id: \"416d77337e24399dc7a5aa058039f72a\", name: \"worker1\", address: \"./bcg/group1/416d77337e24399dc7a5aa058039f72a.socket\" }, range: LongRange, rng: Mutex { data: StdRng { rng: Isaac64Rng {} } } }), work_dir: \".\", rng: Mutex { data: StdRng { rng: Isaac64Rng {} } }, seed: 0, operation_mode: Simulated, protocol: TMembership }";
        
        println!("Worker: {:?}", &worker);
        assert_eq!(format!("{:?}", worker), String::from(default_worker_display));
    }

    //Unit test for: WorkerConfig::write_to_file
    #[test]
    fn test_workerconfig_write_to_file() {
        let mut config = WorkerConfig::new();
        let sr = RadioConfig::new();
        let lr = RadioConfig::new();
        config.radio_short = Some(sr);
        config.radio_long = Some(lr);
    
        let mut path = env::temp_dir();
        path.push("worker.toml");

        let val = config.write_to_file(&path).expect("Could not write configuration file.");
        
        //Assert the file was written.
        assert!(path.exists());

        let expected_file_content = "worker_name = \"worker1\"\nwork_dir = \".\"\nrandom_seed = 0\noperation_mode = \"Simulated\"\nprotocol = \"TMembership\"\n\n[radio_short]\nbroadcast_groups = [\"group1\"]\nreliability = 1.0\ndelay = 0\ninterface_name = \"wlan0\"\n\n[radio_long]\nbroadcast_groups = [\"group1\"]\nreliability = 1.0\ndelay = 0\ninterface_name = \"wlan0\"\n";
        
        let mut file_content = String::new();
        let _res = File::open(path).unwrap().read_to_string(&mut file_content);

        assert_eq!(file_content, expected_file_content);

    }
}