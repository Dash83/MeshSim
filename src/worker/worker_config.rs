//! This module defines the worker_config struct and related functions. It allows meshsim to
//! deserialize a configuration file into a worker_config object that eventually creates a worker object.
extern crate toml;
extern crate rustc_serialize;
extern crate rand;
extern crate byteorder;

use worker::{Worker, OperationMode, Write, WorkerError, DeviceRadio, SimulatedRadio, Radio};
use std::path::Path;
use std::fs::File;
use std::iter;
use self::rustc_serialize::hex::*;
use self::rand::Rng;
use std::sync::{Mutex, Arc};

///Configuration pertaining to a given radio of the worker.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct RadioConfig {
    ///The broadcast groups this radio belongs to. Ignored in device mode.
    pub broadcast_groups : Option<Vec<String>>,
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
        RadioConfig{ broadcast_groups : Some(vec!(String::from("group1"))),
                     reliability : Some(1.0),
                     delay : Some(0),
                     interface_name : Some(String::from("wlan0")),}
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
    ///The configuration for the short-range radio of this worker.
    pub radio_short : RadioConfig,
    ///The configuration for the short-range radio of this worker.
    pub radio_long : RadioConfig,
}

impl WorkerConfig {
    ///Creates a new configuration for a Worker with default settings.
    pub fn new() -> WorkerConfig {
        WorkerConfig{worker_name : String::from("worker1"),
                     work_dir : String::from("."),
                     random_seed : 0, //The random seed itself doesn't need to be random. Also, that makes testing more difficult.
                     operation_mode : OperationMode::Simulated,
                     radio_short : RadioConfig::new(),
                     radio_long : RadioConfig::new(),
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
        //TODO: This warning will dissapear when 2nd radio feature is enabled.
        let (sr, lr) : (Arc<Radio>, Arc<Radio>) = match self.operation_mode {
            OperationMode::Device => { 
                //Short-range radio
                let iname = self.radio_short.interface_name.expect("An interface name for radio_short must be provided when operating in device_mode.");
                let radio_short = DeviceRadio::new(iname, self.worker_name.clone(), id.clone(), Arc::clone(&rng) );

                //Long-range radio
                let iname = self.radio_long.interface_name.expect("An interface name for radio_long must be provided when operating in device_mode.");
                let radio_long = DeviceRadio::new(iname, self.worker_name.clone(), id.clone(), Arc::clone(&rng) );

                (Arc::new(radio_short), Arc::new(radio_long))
            },
            OperationMode::Simulated => { 
                //Short-range radio
                let delay = self.radio_short.delay.unwrap_or(0);
                let reliability = self.radio_short.reliability.unwrap_or(1.0);
                let bg = self.radio_short.broadcast_groups.expect("A list of broadcast groups must be provided for radio_short in simulated_mode.");
                let radio_short = SimulatedRadio::new( delay, 
                                                       reliability, 
                                                       bg, 
                                                       self.work_dir.clone(),
                                                       id.clone(),
                                                       self.worker_name.clone(),
                                                       Arc::clone(&rng) );

                //Long-range radio
                let delay = self.radio_long.delay.unwrap_or(0);
                let reliability = self.radio_long.reliability.unwrap_or(1.0);
                let bg = self.radio_long.broadcast_groups.expect("A list of broadcast groups must be provided for radio_long in simulated_mode.");
                let radio_long = SimulatedRadio::new( delay,
                                                      reliability, 
                                                      bg, 
                                                      self.work_dir.clone(), 
                                                      id.clone(), 
                                                      self.worker_name.clone(),
                                                      Arc::clone(&rng) );

                (Arc::new(radio_short), Arc::new(radio_long))                
            },
        };

        Worker{ name : self.worker_name,
                short_radio : Some(sr),
                //long_radio : lr, //TODO: Uncomment when 2nd radio feature is enabled.
                work_dir : self.work_dir, 
                rng : Arc::clone(&rng),
                seed : self.random_seed,
                operation_mode : self.operation_mode,
                id : id }
    }

    ///Writes the current configuration object to a formatted configuration file, that can be passed to
    ///the worker_cli binary.
    pub fn write_to_file<P: AsRef<Path>>(&self, file_path: P) -> Result<(), WorkerError> {
        //Create configuration file
        //let file_path = format!("{}{}{}", dir, std::path::MAIN_SEPARATOR, file_name);
        let mut file = try!(File::create(&file_path));
        let data = toml::to_string(self).unwrap();
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
        let config_str = "WorkerConfig { worker_name: \"worker1\", work_dir: \".\", random_seed: 0, operation_mode: Simulated, radio_short: RadioConfig { broadcast_groups: Some([\"group1\"]), reliability: Some(1), delay: Some(0), interface_name: Some(\"wlan0\") }, radio_long: RadioConfig { broadcast_groups: Some([\"group1\"]), reliability: Some(1), delay: Some(0), interface_name: Some(\"wlan0\") } }";

        assert_eq!(format!("{:?}", config), config_str);
    }

    //Unit test for: WorkerConfig::create_worker
    #[test]
    fn test_workerconfig_create_worker() {
        //test_workerconfig_new and test_worker_new already test the correct instantiation of WorkerConfig and Worker.
        //Just make sure thing function translates a WorkerConfig correctly into a Worker.
        let conf = WorkerConfig::new();
        let worker = conf.create_worker();
        let default_worker_display = "Worker { name: \"worker1\", id: \"416d77337e24399dc7a5aa058039f72a\", short_radio: Some(SimulatedRadio { delay: 0, reliability: 1, broadcast_groups: [\"group1\"], work_dir: \".\", me: Peer { id: \"416d77337e24399dc7a5aa058039f72a\", name: \"worker1\", address: \"./bcgroups/group1/416d77337e24399dc7a5aa058039f72a.socket\" }, rng: Mutex { data: StdRng { rng: Isaac64Rng {} } } }), work_dir: \".\", rng: Mutex { data: StdRng { rng: Isaac64Rng {} } }, seed: 0, operation_mode: Simulated }";
        
        assert_eq!(format!("{:?}", worker), String::from(default_worker_display));
    }

    //Unit test for: WorkerConfig::write_to_file
    #[test]
    fn test_workerconfig_write_to_file() {
        let config = WorkerConfig::new();
        let mut path = env::temp_dir();
        path.push("worker.toml");

        let val = config.write_to_file(&path).expect("Could not write configuration file.");
        
        //Assert the file was written.
        assert!(path.exists());

        let expected_file_content = "worker_name = \"worker1\"\nwork_dir = \".\"\nrandom_seed = 0\noperation_mode = \"Simulated\"\n\n[radio_short]\nbroadcast_groups = [\"group1\"]\nreliability = 1.0\ndelay = 0\ninterface_name = \"wlan0\"\n\n[radio_long]\nbroadcast_groups = [\"group1\"]\nreliability = 1.0\ndelay = 0\ninterface_name = \"wlan0\"\n";
        
        let mut file_content = String::new();
        let _res = File::open(path).unwrap().read_to_string(&mut file_content);

        assert_eq!(expected_file_content, file_content);

    }
}