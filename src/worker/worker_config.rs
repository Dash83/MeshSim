//! This module defines the worker_config struct and related functions. It allows meshsim to
//! deserialize a configuration file into a worker_config object that eventually creates a worker object.
extern crate toml;
extern crate rustc_serialize;
extern crate rand;
extern crate byteorder;

use crate::worker::{Worker, OperationMode, Write, WorkerError};
use crate::worker::radio::*;
use crate::worker::protocols::*;
use crate::worker::listener::Listener;
use std::path::Path;
use std::fs::File;
use std::iter;
use self::rustc_serialize::hex::*;
use self::rand::{rngs::StdRng, RngCore};
use std::sync::{Mutex, Arc};
use crate::worker::mobility::{Position, Velocity};
use ::slog::Logger;
use crate::worker::radio::{self, WifiRadio, SimulatedRadio, LoraFrequencies};

///Default range in meters for short-range radios
pub const DEFAULT_SHORT_RADIO_RANGE : f64 = 100.0;
///Default range in meters for long-range radios
pub const DEFAULT_LONG_RADIO_RANGE : f64 = 500.0;
///Default range in meters for long-range radios
pub const DEFAULT_INTERFACE_NAME : &str = "wlan";

const DEFAULT_SPREADING_FACTOR : u32 = 0;
const DEFAULT_LORA_FREQ : LoraFrequencies = LoraFrequencies::Europe;
const DEFAULT_LORA_TRANS_POWER : u8 = 15;

//TODO: Cleanup this struct
///Configuration pertaining to a given radio of the worker.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Default)]
pub struct RadioConfig {
    ///Simulated mode only. How likely ([0-1]) are packets to reach their destination.
    pub reliability : Option<f64>,
    ///Name of the network interface that this radio will use.
    pub interface_name : Option<String>,
    ///Range in meters of this radio.
    pub range : f64,
    ///Frequency for the Lora radio. Varies by country.
    pub frequency : Option<u64>,
    ///Spreading factor for the Lora radio.
    pub spreading_factor : Option<u32>,
    ///Transmission power for the Lora radio.
    pub transmission_power : Option<u8>,
}

impl RadioConfig {
    /// Create a new default radio configuration with all fields set to a default. This it not valid for either simulated or device mode,
    /// so users should take care to modify the appropriate values.
    pub fn new() -> RadioConfig {
        RadioConfig{ reliability : Some(1.0),
                     interface_name : Some(format!("{}0", DEFAULT_INTERFACE_NAME)),
                     range : 0.0,
                     frequency : None,
                     spreading_factor : None,
                     transmission_power : None,
                    }
    }

    /// Consuming the underlying configuration, this method produces a Box<Radio> object that can be started and used.
    pub fn create_radio(self, operation_mode : OperationMode, 
                              r_type : RadioTypes,
                              work_dir : String, 
                              worker_name : String, 
                              worker_id : String,
                              seed : u32, 
                              r : Option<Arc<Mutex<StdRng>>>,
                              logger : Logger) -> Result<(Arc<Radio>, Box<Listener>), WorkerError> {
        let rng = match r {
            Some(gen) => gen,
            None => { Arc::new(Mutex::new(Worker::rng_from_seed(seed))) }
        };

        match operation_mode {
            OperationMode::Device => { 
                let iname = self.interface_name.expect("An interface name for radio_short must be provided when operating in device_mode.");
                let (radio, listener) : (Arc<Radio>, Box<Listener>) = match r_type {
                    RadioTypes::ShortRange => {
                        let (r, l) = WifiRadio::new(iname,
                                       worker_name,
                                       worker_id,
                                       rng,
                                       r_type,
                                       logger)?;
                        (Arc::new(r), l)
                    },
                    RadioTypes::LongRange => {
                        let sf = self.spreading_factor.unwrap_or(DEFAULT_SPREADING_FACTOR);
                        let freq = self.frequency.unwrap_or(DEFAULT_LORA_FREQ as u64);
                        let power = self.transmission_power.unwrap_or(DEFAULT_LORA_TRANS_POWER);
                        let (r, l) = radio::new_lora_radio(freq, sf, power)?;
                        (r, l)
                    },
                };
                Ok((radio, listener))
            },
            OperationMode::Simulated => { 
                //let delay = self.delay.unwrap_or(0);
                let reliability = self.reliability.unwrap_or(1.0);
                //let bg = self.broadcast_groups;
                let (radio, listener) = SimulatedRadio::new(reliability, 
                                                            work_dir,
                                                            worker_id,
                                                            worker_name,
                                                            r_type,
                                                            self.range,
                                                            rng,
                                                            logger)?;
                Ok((Arc::new(radio), listener))
            },
        }
    }
}

/// Configuration for a worker object. This struct encapsulates several of the properties
/// of a worker. Its use is for the external interfaces of the worker module. This facilitates
/// that an external client such as worker_cli can create WorkerConfig objects from CLI parameters
/// or configuration files and pass the configuration around, leaving the actual construction of
/// the worker object to the worker module.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Default)]
pub struct WorkerConfig {
    ///Name of the worker.
    pub worker_name : String,
    ///Unique id for the worker.
    pub worker_id : Option<String>,
    ///Directory for the worker to operate. Must have RW access to it. Operational files and 
    ///log files will be written here.
    pub work_dir : String,
    ///Random seed used for all RNG operations.
    pub random_seed : u32,
    ///Simulated or Device operation.
    pub operation_mode : OperationMode,
    /// Should this worker accept commands
    pub accept_commands : Option<bool>,
    /// Should this worker log output to the terminal
    pub term_log : Option<bool>,
    /// The protocol that this Worker should run for this configuration.
    pub protocol : Protocols,
    /// Initial position of the worker
    #[serde(flatten)]
    pub position : Position,
    /// Optional field used for mobility models.
    pub destination : Option<Position>,
    /// Velocity vector of the worker
    pub velocity : Velocity,
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
                     worker_id : None,
                     random_seed : 0, //The random seed itself doesn't need to be random. Also, that makes testing more difficult.
                     operation_mode : OperationMode::Simulated,
                     accept_commands : None,
                     term_log : None,
                     protocol : Protocols::NaiveRouting,
                     radio_short : None,
                     radio_long : None,
                     position : Position{ x : 0.0, y : 0.0 },
                     destination : None,
                     velocity : Velocity{ x : 0.0, y : 0.0 },
        }
    }

    ///Creates a new Worker object configured with the values of this configuration object.
    pub fn create_worker(self, logger: Logger) -> Result<Worker, WorkerError> {
        //Create the RNG
        let gen = Worker::rng_from_seed(self.random_seed);
        //Check if a worker_id is present
        let id = match self.worker_id { 
            Some(id) => id.clone(),
            None => WorkerConfig::gen_id(self.random_seed)
        };
        //Wrap the rng in the shared-mutable-state smart pointers
        let rng = Arc::new(Mutex::new(gen));

        //Create the radios
        let sr_channels = match self.radio_short {
            Some(sr_config) => { 
                let (sr, listener) = sr_config.create_radio(self.operation_mode, 
                                                RadioTypes::ShortRange, 
                                                self.work_dir.clone(), 
                                                self.worker_name.clone(), 
                                                id.clone(), 
                                                self.random_seed, 
                                                Some(Arc::clone(&rng)),
                                                logger.clone())?;
                Some((sr, listener))
            },
            None => { None }
        };

        let lr_channels = match self.radio_long {
            Some(lr_config) => { 
                let (lr, listener) = lr_config.create_radio(self.operation_mode, 
                                                RadioTypes::LongRange, 
                                                self.work_dir.clone(), 
                                                self.worker_name.clone(),
                                                id.clone(), 
                                                self.random_seed, 
                                                Some(Arc::clone(&rng)),
                                                logger.clone())?;
                Some((lr, listener))
            },
            None => { None }
        };

        //Need to add an endline char to stdout after both radios have been initialized.
        println!();
        
        // if self.operation_mode == OperationMode::Simulated &&
        //    register_worker == true {
        //     let conn = mobility::get_db_connection(&self.work_dir, &logger)?;
        //     // debug!("DB Connection obtained.");
        //     let _rows = mobility::create_db_objects(&conn, &logger)?;
        //     // debug!("create_positions_db returned {}", _rows);
        //     let db_id = mobility::register_worker(&conn, self.worker_name.clone(), 
        //                                             &id, 
        //                                             &self.position, 
        //                                             &self.velocity, 
        //                                             &self.destination,
        //                                             sr_addr, 
        //                                             lr_addr,
        //                                             &logger)?;
        //     debug!(logger, "Worker registered correcly with id {}", &db_id);
        // }


        let w = Worker{ name : self.worker_name,
                        short_radio : sr_channels,
                        long_radio : lr_channels,
                        work_dir : self.work_dir, 
                        rng : Arc::clone(&rng),
                        seed : self.random_seed,
                        operation_mode : self.operation_mode,
                        id,
                        protocol : self.protocol,
                        // db_id : db_id,
                        logger };
        Ok(w)
    }

    ///Writes the current configuration object to a formatted configuration file, that can be passed to
    ///the worker_cli binary.
    pub fn write_to_file<P: AsRef<Path>>(&self, file_path: P) -> Result<(), WorkerError> {
        //Create configuration file
        let mut file = File::create(&file_path)?;
        let data = match toml::to_string(self) {
            Ok(d) => d,
            Err(e) => return Err(WorkerError::Configuration(format!("Error writing configuration to file: {}", e)))
        };
        write!(file, "{}", data)?;

        Ok(())
    }

    ///Creates a worker_id based on a random seed.
    pub fn gen_id(seed : u32) -> String {
        let mut gen = Worker::rng_from_seed(seed);
        
        //Vector of 16 bytes set to 0
        let mut key : Vec<u8>= iter::repeat(0u8).take(16).collect();
        //Fill the vector with 16 random bytes.
        gen.fill_bytes(&mut key[..]);
        key.to_hex().to_string()
    }
}

//**** WorkerConfig unit tests ****
#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    use super::*;
    use std::env;
    use crate::logging;

    //Unit test for: WorkerConfig_new
    #[test]
    fn test_workerconfig_new() {
        let config = WorkerConfig::new();
        let config_str = "WorkerConfig { worker_name: \"worker1\", worker_id: None, work_dir: \".\", random_seed: 0, operation_mode: Simulated, accept_commands: None, term_log: None, protocol: NaiveRouting, position: Position { x: 0.0, y: 0.0 }, destination: None, velocity: Velocity { x: 0.0, y: 0.0 }, radio_short: None, radio_long: None }";

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
        let log_file = "/tmp/test.log";
        let logger = logging::create_logger(log_file, false).expect("Could not create logger");

        let res = config.create_worker(logger);
        
        assert!(res.is_ok());
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

        let _val = config.write_to_file(&path).expect("Could not write configuration file.");
        
        //Assert the file was written.
        assert!(path.exists());

        let expected_file_content = "worker_name = \"worker1\"\nwork_dir = \".\"\nrandom_seed = 0\noperation_mode = \"Simulated\"\nprotocol = \"NaiveRouting\"\nx = 0.0\ny = 0.0\n\n[velocity]\nx = 0.0\ny = 0.0\n\n[radio_short]\nreliability = 1.0\ninterface_name = \"wlan0\"\nrange = 0.0\n\n[radio_long]\nreliability = 1.0\ninterface_name = \"wlan0\"\nrange = 0.0\n";
        
        let mut file_content = String::new();
        let _res = File::open(path).unwrap().read_to_string(&mut file_content);

        assert_eq!(file_content, expected_file_content);

    }
}