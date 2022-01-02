//! This module defines the worker_config struct and related functions. It allows meshsim to
//! deserialize a configuration file into a worker_config object that eventually creates a worker object.

use crate::mobility::{Position, Velocity};
use crate::worker::listener::Listener;
use crate::worker::protocols::*;
use crate::worker::radio::*;
use crate::worker::radio::{self, LoraFrequencies, SimulatedRadio, WifiRadio};
use crate::worker::{Channel, OperationMode, Worker, Write};
use crate::{MeshSimError, MeshSimErrorKind};
use rand::SeedableRng;
use rand::{rngs::StdRng, RngCore};
use crate::common::*;
use slog::Logger;
use std::fs::File;
use std::iter;
use std::path::Path;
use std::sync::{Arc, Mutex};

///Default range in meters for short-range radios
pub const DEFAULT_SHORT_RADIO_RANGE: f64 = 100.0;
///Default range in meters for long-range radios
pub const DEFAULT_LONG_RADIO_RANGE: f64 = 500.0;
///Default range in meters for long-range radios
pub const DEFAULT_INTERFACE_NAME: &str = "wlan";

const DEFAULT_SPREADING_FACTOR: u32 = 0;
const DEFAULT_LORA_FREQ: LoraFrequencies = LoraFrequencies::Europe;
const DEFAULT_WIFI_FREQ: u64 = 2400; //In mhz
const DEFAULT_LORA_TRANS_POWER: u8 = 15;
const DEFAULT_WIFI_TRANS_POWER: u8 = 30; //In decibel-milliwatts (dBm)
const DEFAULT_PACKET_QUEUE_SIZE: usize = 3000; //Max number of queued packets
const DEFAULT_READ_TIMEOUT: u64 = 1_000_000; //1ms in nanoseconds
const DEFAULT_STALE_PACKET_THRESHOLD: i64 = 3_000_000_000; //3 seconds in nanoseconds
const DEFAULT_POWER_LOSS_COEFFICIENT: f64 = 30.0f64;
const DEFAULT_REFERENCE_DISTANCE: f64 = 1.0f64; //In meters
const DEFAULT_FLOOR_PENETRATION_LOSS_FACTOR: f64 = 14.0f64; //In dBm
//Default frequency for the Short Radio



//TODO: Cleanup this struct
///Configuration pertaining to a given radio of the worker.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Default)]
pub struct RadioConfig {
    ///Timeout (in ms) for each network read.
    pub timeout: Option<u64>,
    ///Name of the network interface that this radio will use.
    pub interface_name: Option<String>,
    ///Range in meters of this radio.
    pub range: f64,
    ///Max number of retries for the mac layer CDMA/CA algorithm
    pub mac_layer_retries: Option<usize>,
    ///The base wait-number in milliseconds for each time the mac layer has a collition
    pub mac_layer_base_wait: Option<u64>,
    ///Frequency for the Lora radio. Varies by country.
    pub frequency: Option<u64>,
    ///Spreading factor for the Lora radio.
    pub spreading_factor: Option<u32>,
    ///Transmission power for the Lora radio.
    pub transmission_power: Option<u8>,
    // Power loss coefficient used to calculate the transmission power.
    pub power_loss_coefficient: Option<f64>,
    // Reference distance used to calculate the transmission power.
    pub reference_distance: Option<f64>,
    // Floor penetration factor used to calculate the transmission power.
    pub floor_penetration_loss_factor: Option<f64>,
}

impl RadioConfig {
    /// Create a new default radio configuration with all fields set to a default. This it not valid for either simulated or device mode,
    /// so users should take care to modify the appropriate values.
    pub fn new() -> RadioConfig {
        RadioConfig {
            timeout: Some(DEFAULT_READ_TIMEOUT),
            interface_name: Some(format!("{}0", DEFAULT_INTERFACE_NAME)),
            range: 0.0,
            frequency: Some(DEFAULT_WIFI_FREQ),
            spreading_factor: None,
            transmission_power: Some(DEFAULT_WIFI_TRANS_POWER),
            mac_layer_retries: Some(DEFAULT_TRANSMISSION_MAX_RETRY),
            mac_layer_base_wait: Some(DEFAULT_TRANSMISSION_WAIT_BASE),
            power_loss_coefficient: Some(DEFAULT_POWER_LOSS_COEFFICIENT),
            reference_distance: Some(DEFAULT_REFERENCE_DISTANCE),
            floor_penetration_loss_factor: Some(DEFAULT_FLOOR_PENETRATION_LOSS_FACTOR),
        }
    }

    /// Consuming the underlying configuration, this method produces a Box<Radio> object that can be started and used.
    pub fn create_radio(
        self,
        operation_mode: OperationMode,
        r_type: RadioTypes,
        work_dir: String,
        worker_name: String,
        worker_id: String,
        seed: u64,
        r: Option<Arc<Mutex<StdRng>>>,
        logger: Logger,
    ) -> Result<Channel, MeshSimError> {
        let rng = match r {
            Some(gen) => gen,
            None => Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
        };

        match operation_mode {
            OperationMode::Device => {
                let iname = self.interface_name.expect("An interface name for radio_short must be provided when operating in device_mode.");
                let (radio, listener): (Arc<dyn Radio>, Box<dyn Listener>) = match r_type {
                    RadioTypes::ShortRange => {
                        let timeout = self.timeout.unwrap_or(DEFAULT_READ_TIMEOUT);
                        let (r, l) = WifiRadio::new(
                            iname,
                            worker_name,
                            worker_id,
                            timeout,
                            rng,
                            r_type,
                            logger,
                        )?;
                        (Arc::new(r), l)
                    }
                    RadioTypes::LongRange => {
                        let sf = self.spreading_factor.unwrap_or(DEFAULT_SPREADING_FACTOR);
                        let freq = self.frequency.unwrap_or(DEFAULT_LORA_FREQ as u64);
                        let power = self.transmission_power.unwrap_or(DEFAULT_LORA_TRANS_POWER);
                        let (r, l) = radio::new_lora_radio(freq, sf, power)?;
                        (r, l)
                    }
                };
                Ok((radio, listener))
            }
            OperationMode::Simulated => {
                let timeout = self.timeout.unwrap_or(DEFAULT_READ_TIMEOUT);
                let mac_layer_retries = self
                    .mac_layer_retries
                    .unwrap_or(DEFAULT_TRANSMISSION_MAX_RETRY);
                let mac_layer_base_wait = self
                    .mac_layer_base_wait
                    .unwrap_or(DEFAULT_TRANSMISSION_WAIT_BASE);
                let transmission_power = self
                    .transmission_power
                    .unwrap_or(DEFAULT_WIFI_TRANS_POWER);
                let power_loss_coefficient = self
                    .power_loss_coefficient
                    .unwrap_or(DEFAULT_POWER_LOSS_COEFFICIENT);
                let reference_distance = self
                    .reference_distance
                    .unwrap_or(DEFAULT_REFERENCE_DISTANCE);
                let floor_penetration_loss_factor = self
                    .floor_penetration_loss_factor
                    .unwrap_or(DEFAULT_FLOOR_PENETRATION_LOSS_FACTOR);
                let frequency_in_mhz = self
                    .frequency
                    .unwrap_or_else(|| {
                        match r_type {
                            RadioTypes::ShortRange => DEFAULT_WIFI_FREQ,
                            RadioTypes::LongRange => DEFAULT_LORA_FREQ as u64,
                        }
                    });
                let frequency_in_mhz = frequency_in_mhz as f64;
                let (radio, listener) = SimulatedRadio::new(
                    timeout,
                    work_dir,
                    worker_id,
                    worker_name,
                    r_type,
                    self.range,
                    mac_layer_retries,
                    mac_layer_base_wait,
                    transmission_power,
                    power_loss_coefficient,
                    reference_distance,
                    floor_penetration_loss_factor,
                    frequency_in_mhz,
                    rng,
                    logger,
                )?;
                Ok((Arc::new(radio), listener))
            }
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
    pub worker_name: String,
    ///Unique id for the worker.
    pub worker_id: Option<String>,
    ///Directory for the worker to operate. Must have RW access to it. Operational files and
    ///log files will be written here.
    pub work_dir: String,
    ///Random seed used for all RNG operations.
    pub random_seed: u64,
    ///Simulated or Device operation.
    pub operation_mode: OperationMode,
    /// Should this worker accept commands
    pub accept_commands: Option<bool>,
    /// Should this worker log output to the terminal
    pub term_log: Option<bool>,
    /// The maximum number of queued packets a worker can have
    pub packet_queue_size: Option<usize>,
    ///Threshold after which a packet is considered stale and dropped.
    ///Expressed in nanoseconds.
    pub stale_packet_threshold: Option<i64>,
    ///NOTE: Due to the way serde_toml works, the following fields must be kept last in the structure.
    /// Initial position of the worker
    //    #[serde(flatten)]
    pub position: Position,
    /// Optional field used for mobility models.
    pub destination: Option<Position>,
    /// Velocity vector of the worker
    pub velocity: Velocity,
    /// The protocol that this Worker should run for this configuration.
    pub protocol: Option<Protocols>,
    /// This is because they are interpreted as TOML tables, and those are always placed at the end of structures.
    ///The configuration for the short-range radio of this worker.
    pub radio_short: Option<RadioConfig>,
    ///The configuration for the short-range radio of this worker.
    pub radio_long: Option<RadioConfig>,
}

// impl Ord for WorkerConfig {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         todo!()
//     }

//     fn max(self, other: Self) -> Self
//     where
//         Self: Sized,
//     {
//         std::cmp::max_by(self, other, Ord::cmp)
//     }

//     fn min(self, other: Self) -> Self
//     where
//         Self: Sized,
//     {
//         std::cmp::min_by(self, other, Ord::cmp)
//     }

//     fn clamp(self, min: Self, max: Self) -> Self
//     where
//         Self: Sized,
//     {
//         assert!(min <= max);
//         if self < std::cmp::min {
//             std::cmp::min
//         } else if self > std::cmp::max {
//             std::cmp::max
//         } else {
//             self
//         }
//     }
// }

// impl PartialOrd for WorkerConfig {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         match self.worker_name.partial_cmp(&other.worker_name) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.worker_id.partial_cmp(&other.worker_id) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.work_dir.partial_cmp(&other.work_dir) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.random_seed.partial_cmp(&other.random_seed) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.operation_mode.partial_cmp(&other.operation_mode) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.accept_commands.partial_cmp(&other.accept_commands) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.term_log.partial_cmp(&other.term_log) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.packet_queue_size.partial_cmp(&other.packet_queue_size) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.stale_packet_threshold.partial_cmp(&other.stale_packet_threshold) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.position.partial_cmp(&other.position) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.destination.partial_cmp(&other.destination) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.velocity.partial_cmp(&other.velocity) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.protocol.partial_cmp(&other.protocol) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         match self.radio_short.partial_cmp(&other.radio_short) {
//             Some(core::cmp::Ordering::Equal) => {}
//             ord => return ord,
//         }
//         self.radio_long.partial_cmp(&other.radio_long)
//     }
// }

impl WorkerConfig {
    ///Creates a new configuration for a Worker with default settings.
    pub fn new() -> WorkerConfig {
        WorkerConfig {
            worker_name: String::from("worker1"),
            work_dir: String::from("."),
            worker_id: None,
            random_seed: 0, //The random seed itself doesn't need to be random. Also, that makes testing more difficult.
            operation_mode: OperationMode::Simulated,
            accept_commands: None,
            term_log: None,
            packet_queue_size: Some(DEFAULT_PACKET_QUEUE_SIZE),
            stale_packet_threshold: Some(DEFAULT_STALE_PACKET_THRESHOLD),
            protocol: Some(Protocols::Flooding),
            radio_short: None,
            radio_long: None,
            position: Position { x: 0.0, y: 0.0 },
            destination: None,
            velocity: Velocity { x: 0.0, y: 0.0 },
        }
    }

    ///Creates a new Worker object configured with the values of this configuration object.
    pub fn create_worker(self, logger: Logger) -> Result<Worker, MeshSimError> {
        //Create the RNG
        let mut gen = StdRng::seed_from_u64(self.random_seed);
        //Check if a worker_id is present
        let id = match self.worker_id {
            Some(id) => id,
            None => generate_worker_id(&mut gen),
        };
        //Wrap the rng in the shared-mutable-state smart pointers
        let rng = Arc::new(Mutex::new(gen));
        let protocol = self.protocol.expect("A protocol must be specified");

        //Create the radios
        let sr_channels = match self.radio_short {
            Some(sr_config) => {
                let (sr, listener) = sr_config.create_radio(
                    self.operation_mode,
                    RadioTypes::ShortRange,
                    self.work_dir.clone(),
                    self.worker_name.clone(),
                    id.clone(),
                    self.random_seed,
                    Some(Arc::clone(&rng)),
                    logger.clone(),
                )?;
                Some((sr, listener))
            }
            None => None,
        };

        let lr_channels = match self.radio_long {
            Some(lr_config) => {
                let (lr, listener) = lr_config.create_radio(
                    self.operation_mode,
                    RadioTypes::LongRange,
                    self.work_dir.clone(),
                    self.worker_name.clone(),
                    id.clone(),
                    self.random_seed,
                    Some(Arc::clone(&rng)),
                    logger.clone(),
                )?;
                Some((lr, listener))
            }
            None => None,
        };

        // //Need to add an endline char to stdout after both radios have been initialized.
        // println!();

        Worker::new(
            self.worker_name,
            sr_channels,
            lr_channels,
            self.work_dir,
            Arc::clone(&rng),
            self.random_seed,
            self.operation_mode,
            id,
            protocol,
            self.packet_queue_size.unwrap_or(DEFAULT_PACKET_QUEUE_SIZE),
            self.stale_packet_threshold.unwrap_or(DEFAULT_STALE_PACKET_THRESHOLD),
            self.position,
            Some(self.velocity),
            self.destination,
            logger,
        )
    }

    ///Writes the current configuration object to a formatted configuration file, that can be passed to
    ///the worker_cli binary.
    pub fn write_to_file<P: AsRef<Path>>(&self, file_path: P) -> Result<(), MeshSimError> {
        let data = toml::to_string(self).map_err(|e| {
            let err_msg = String::from("Failed to serialize configuration data");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        File::create(&file_path)
            .and_then(|mut file| write!(file, "{}", data))
            .map_err(|e| {
                let err_msg = String::from("Error writing configuration to file");
                MeshSimError {
                    kind: MeshSimErrorKind::Configuration(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;

        Ok(())
    }

}

//**** WorkerConfig unit tests ****
#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging;
    use std::env;
    use std::fs::File;
    use std::io::Read;

    //Unit test for: WorkerConfig_new
    #[test]
    fn test_workerconfig_new() {
        let config = WorkerConfig::new();
        let config_str = "\
        WorkerConfig { \
            worker_name: \"worker1\", \
            worker_id: None, \
            work_dir: \".\", \
            random_seed: 0, \
            operation_mode: Simulated, \
            accept_commands: None, \
            term_log: None, \
            packet_queue_size: Some(3000), \
            stale_packet_threshold: Some(3000000000), \
            position: Position { x: 0.0, y: 0.0 }, \
            destination: None, \
            velocity: Velocity { x: 0.0, y: 0.0 }, \
            protocol: Some(Flooding), \
            radio_short: None, \
            radio_long: None }";

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

        let _val = config
            .write_to_file(&path)
            .expect("Could not write configuration file.");

        //Assert the file was written.
        assert!(path.exists());

        let expected_file_content = "\
        worker_name = \"worker1\"\n\
        work_dir = \".\"\n\
        random_seed = 0\n\
        operation_mode = \"Simulated\"\n\
        packet_queue_size = 3000\n\
        stale_packet_threshold = 3000000000\n\
        \n[position]\n\
        x = 0.0\n\
        y = 0.0\n\
        \n\
        [velocity]\n\
        x = 0.0\n\
        y = 0.0\n\
        \n\
        [protocol]\n\
        Protocol = \"Flooding\"\n\
        \n\
        [radio_short]\n\
        timeout = 1000000\n\
        interface_name = \"wlan0\"\n\
        range = 0.0\n\
        mac_layer_retries = 8\n\
        mac_layer_base_wait = 16\n\
        frequency = 2400\n\
        transmission_power = 30\n\
        power_loss_coefficient = 30.0\n\
        reference_distance = 1.0\n\
        floor_penetration_loss_factor = 14.0\n\
        \n\
        [radio_long]\n\
        timeout = 1000000\n\
        interface_name = \"wlan0\"\n\
        range = 0.0\n\
        mac_layer_retries = 8\n\
        mac_layer_base_wait = 16\n\
        frequency = 2400\n\
        transmission_power = 30\n\
        power_loss_coefficient = 30.0\n\
        reference_distance = 1.0\n\
        floor_penetration_loss_factor = 14.0\n";
        
        let mut file_content = String::new();
        let _res = File::open(path).unwrap().read_to_string(&mut file_content);

        assert_eq!(file_content, expected_file_content);
    }
}
