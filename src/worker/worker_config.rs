//! This module defines the worker_config struct and related functions. It allows meshsim to
//! deserialize a configuration file into a worker_config object that eventually creates a worker object.

use worker::{Worker, SIMULATED_SCAN_DIR, DNS_SERVICE_PORT, OperationMode, Write, WorkerError, Radio};
use std::path::Path;
use std::fs::File;

/// Configuration for a worker object. This struct encapsulates several of the properties
/// of a worker. Its use is for the external interfaces of the worker module. This facilitates
/// that an external client such as worker_cli can create WorkerConfig objects from CLI parameters
/// or configuration files and pass the configuration around, leaving the actual construction of
/// the worker object to the worker module.
#[derive(Debug, Deserialize, PartialEq)]
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
    ///The broadcast groups this worker belongs to. Ignored in device mode.
    pub broadcast_groups : Option<Vec<String>>,
    ///Simulated mode only. How likely ([0-1]) are packets to reach their destination.
    pub reliability : Option<f64>,
    ///Simulated mode only. Artificial delay (in ms) introduced to the network packets of this node.
    pub delay : Option<u32>,
    ///How often (ms) should the worker scan for new peers.
    pub scan_interval : Option<u32>,
    ///Name of the network interface that the worker will use.
    pub interface_name : String,

    
}

impl WorkerConfig {
    ///Creates a new configuration for a Worker with default settings.
    pub fn new() -> WorkerConfig {
        WorkerConfig{worker_name : String::from("worker1"),
                     work_dir : String::from("."),
                     random_seed : 0, //The random seed itself doesn't need to be random. Also, that makes testing more difficult.
                     operation_mode : OperationMode::Simulated,
                     broadcast_groups : Some(vec!(String::from("group1"))),
                     reliability : Some(1.0),
                     delay : Some(0),
                     scan_interval : Some(2000),
                     interface_name : String::from("wlan0"),
                    }
    }

    ///Creates a new Worker object configured with the values of this configuration object.
    pub fn create_worker(self) -> Worker {
        let mut obj = Worker::new();
        obj.me.name = self.worker_name;
        obj.me.address_type = self.operation_mode.clone();
        obj.work_dir = self.work_dir;
        obj.random_seed = self.random_seed;
        obj.operation_mode = self.operation_mode;
        if obj.operation_mode == OperationMode::Device {
            let address = Radio::get_radio_address(&self.interface_name).expect("Could not get address for specified interface.");
            debug!("Obtained address {}", &address);
            obj.me.address = format!("{}:{}", address, DNS_SERVICE_PORT);
        } else {
            obj.me.address = format!("{}/{}/{}.socket", obj.work_dir, SIMULATED_SCAN_DIR, obj.me.public_key);
        }

        obj.short_radio.broadcast_groups = self.broadcast_groups.unwrap_or(vec![]);
        obj.short_radio.reliability = self.reliability.unwrap_or(obj.short_radio.reliability);
        obj.short_radio.delay = self.delay.unwrap_or(obj.short_radio.delay);
        obj.scan_interval = self.scan_interval.unwrap_or(obj.scan_interval);
        
        obj
    }

    ///Writes the current configuration object to a formatted configuration file, that can be passed to
    ///the worker_cli binary.
    pub fn write_to_file(&self, file_path : &Path) -> Result<String, WorkerError> {
        //Create configuration file
        //let file_path = format!("{}{}{}", dir, std::path::MAIN_SEPARATOR, file_name);
        let mut file = try!(File::create(&file_path));
        let groups = self.broadcast_groups.as_ref().cloned().unwrap_or(Vec::new());

        //Write content to file
        //file.write(sample_toml_str.as_bytes()).expect("Error writing to toml file.");
        write!(file, "worker_name = \"{}\"\n", self.worker_name)?;
        write!(file, "random_seed = {}\n", self.random_seed)?;
        write!(file, "work_dir = \"{}\"\n", self.work_dir)?;
        write!(file, "operation_mode = \"{}\"\n", self.operation_mode)?;
        write!(file, "reliability = {}\n", self.reliability.unwrap_or(1f64))?;
        write!(file, "delay = {}\n", self.delay.unwrap_or(0u32))?;
        write!(file, "scan_interval = {}\n", self.scan_interval.unwrap_or(1000u32))?;
        write!(file, "broadcast_groups = {:?}\n", groups)?;
        write!(file, "interface_name = {:?}\n", "wlan0")?;

        //file.flush().expect("Error flusing toml file to disk.");
        let file_name = format!("{}", file_path.display());
        Ok(file_name)
    }
}

//**** WorkerConfig unit tests ****
#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::fs::File;
    use std::io::Read;
    use super::*;
    use std::env;

    //Unit test for: WorkerConfig_new
    #[test]
    fn test_workerconfig_new() {
        let config = WorkerConfig::new();
        let config_str = "WorkerConfig { worker_name: \"worker1\", work_dir: \".\", random_seed: 0, operation_mode: Simulated, broadcast_groups: Some([\"group1\"]), reliability: Some(1), delay: Some(0), scan_interval: Some(2000), interface_name: \"wlan0\" }";

        assert_eq!(format!("{:?}", config), config_str);
    }

    //Unit test for: WorkerConfig::create_worker
    #[test]
    fn test_workerconfig_create_worker() {
        //test_workerconfig_new and test_worker_new already test the correct instantiation of WorkerConfig and Worker.
        //Just make sure thing function translates a WorkerConfig correctly into a Worker.
        let conf = WorkerConfig::new();
        let worker = conf.create_worker();
        let default_worker_display = "Worker { short_radio: Radio { delay: 0, reliability: 1, broadcast_groups: [\"group1\"], radio_name: \"\" }, long_radio: Radio { delay: 0, reliability: 1, broadcast_groups: [], radio_name: \"\" }, nearby_peers: {}, me: Peer { public_key: \"00000000000000000000000000000000\", name: \"worker1\", address: \"./bcgroups/00000000000000000000000000000000.socket\", address_type: Simulated }, work_dir: \".\", random_seed: 0, operation_mode: Simulated, scan_interval: 2000, global_peer_list: Mutex { data: {} }, suspected_list: Mutex { data: [] } }";
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
        assert_eq!(val, format!("{}", path.display()));
        assert!(path.exists());

        let expected_file_content = "worker_name = \"worker1\"\nrandom_seed = 0\nwork_dir = \".\"\noperation_mode = \"Simulated\"\nreliability = 1\ndelay = 0\nscan_interval = 2000\nbroadcast_groups = [\"group1\"]\ninterface_name = \"wlan0\"\n";
        let mut file_content = String::new();
        File::open(path).unwrap().read_to_string(&mut file_content);

        assert_eq!(expected_file_content, file_content);

    }
}