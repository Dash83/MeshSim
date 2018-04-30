//! This module defines the abstraction and functionality for what a Radio is in MeshSim

extern crate pnet;
extern crate ipnetwork;

use worker::*;

///Types of radio supported by the system. Used by Protocols that need to 
/// request an operation from the worker on a given radio.
#[derive(Debug)]
pub enum RadioTypes{
    ///Represents the longer-range radio amongst the available ones.
    LongRange,
    ///Represents the short-range, data radio.
    ShortRange,
}

/// Trait for all types of radios.
pub trait Radio {
    ///Method that implements the radio-specific logic to send data over the network.
    fn send(&self, msg : MessageHeader) -> Result<(), WorkerError>;
    ///Method that implements the radio-specific logic to scan it's medium for other nodes.
    fn scan_for_peers(&self) -> Result<HashSet<Peer>, WorkerError>;
}

/// Represents a radio used by the worker to send a message to the network.
#[derive(Debug)]
pub struct SimulatedRadio {
    /// delay parameter used by the test. Sets a number of millisecs
    /// as base value for delay of messages. The actual delay for message sending 
    /// will be a a percentage between the constants MESSAGE_DELAY_LOW and MESSAGE_DELAY_HIGH
    pub delay : u32,
    /// Reliability parameter used by the test. Sets a number of percentage
    /// between (0 and 1] of probability that the message sent by this worker will
    /// not reach its destination.
    pub reliability : f64,
    ///Broadcast group for this radio. Only used in simulated mode.
    pub broadcast_groups : Vec<String>,
    ///Name of the network interface that maps to this Radio object.
    pub radio_name : String,
}

impl Radio  for SimulatedRadio {
    /// Send a Worker::Message over the address implemented by the current Radio.
    fn send(&self, msg : MessageHeader) -> Result<(), WorkerError> {
        //should the message be sent?

        //should the message be delayed?

        // match &msg.destination.address_type {
        //     &OperationMode::Simulated => self.send_simulated_mode(msg) ,
        //     &OperationMode::Device => self.send_device_mode(msg),
        // }
        Ok(())
    }

    fn scan_for_peers(&self) -> Result<HashSet<Peer>, WorkerError> {
        Ok(HashSet::new())
    }
}

impl SimulatedRadio {
    // fn send_simulated_mode(&self, msg : MessageHeader) -> Result<(), WorkerError> {
    //     let mut socket = try!(UnixStream::connect(&msg.destination.address));
      
    //     //info!("Sending message to {}, address {}.", destination.name, destination.address);
    //     let data = try!(to_vec(&msg));
    //     try!(socket.write_all(&data));
    //     info!("Message sent successfully.");
    //     Ok(())
    // }

    // fn send_device_mode(&self, msg : MessageHeader) -> Result<(), WorkerError> {
    //     let mut socket = try!(TcpStream::connect(&msg.destination.address));
        
    //     //info!("Sending message to {}, address {}.", destination.name, destination.address);
    //     let data = try!(to_vec(&msg));
    //     try!(socket.write_all(&data));
    //     info!("Message sent successfully.");
    //     Ok(())
    // }

    /// Constructor for new Radios
    // pub fn new() -> Radio {
    //     Radio{ delay : 0,
    //            reliability : 1.0,
    //            broadcast_groups : vec![],
    //            radio_name : String::from("") }
    // }

    ///Function for adding broadcast groups in simulated mode
    pub fn add_bcast_group(&mut self, group: String) {
        self.broadcast_groups.push(group);
    }
}

/// A radio object that maps directly to a network interface of the system.
#[derive(Debug)]
pub struct DeviceRadio {
    ///Name of the network interface that maps to this Radio object.
    pub radio_name : String,
}

impl Radio  for DeviceRadio{
    /// Send a Worker::Message over the address implemented by the current Radio.
    fn send(&self, msg : MessageHeader) -> Result<(), WorkerError> {
        //should the message be sent?

        //should the message be delayed?

        // match &msg.destination.address_type {
        //     &OperationMode::Simulated => self.send_simulated_mode(msg) ,
        //     &OperationMode::Device => self.send_device_mode(msg),
        // }
        Ok(())
    }

    fn scan_for_peers(&self) -> Result<HashSet<Peer>, WorkerError> {
        Ok(HashSet::new())
    }
}

impl DeviceRadio {
    /// Get the public address of the OS-NIC that maps to this Radio object.
    /// It will return the first IPv4 address from a NIC that exactly matches the name.
    pub fn get_radio_address<'a>(name : &'a str) -> Result<String, WorkerError> {
        use self::pnet::datalink;
        use self::ipnetwork;

        for iface in datalink::interfaces() {
            if &iface.name == name {
                for address in iface.ips {
                    match address {
                        ipnetwork::IpNetwork::V4(addr) => {
                            return Ok(addr.ip().to_string())
                        },
                        ipnetwork::IpNetwork::V6(_) => { /*Only using IPv4 for the moment*/ },
                    }
                }
            }
        }
        Err(WorkerError::Configuration(String::from("Network interface specified in configuration not found.")))
    }
}

    // //Unit test for: Radio::new
    // #[test]
    // fn test_radio_new() {
    //     let radio = Radio::new();
    //     let radio_string = "Radio { delay: 0, reliability: 1, broadcast_groups: [], radio_name: \"\" }";

    //     assert_eq!(format!("{:?}", radio), String::from(radio_string));
    // }

    // //Unit test for: Radio::add_bcast_group
    // #[test]
    // fn test_radio_add_bcast_group() {
    //     let mut radio = Radio::new();
    //     radio.add_bcast_group(String::from("group1"));

    //     assert_eq!(radio.broadcast_groups, vec![String::from("group1")]);
    // }

    //Unit test for: Radio::scan_for_peers
    //#[test]
    /*
    fn test_radio_scan_for_peers() {
        let mut worker = Worker::new();
        //3 phony groups
        worker.radios[0].add_bcast_group(String::from("group1"));
        worker.radios[0].add_bcast_group(String::from("group2"));
        worker.radios[0].add_bcast_group(String::from("group3"));

        //Create dirs
        let mut dir = Path::new("/tmp/scan_bcast_groups").to_path_buf();
        if !dir.exists() {
            let _ = fs::create_dir(&dir).unwrap();
        } else {
            //Directory structure exists. Possibly from an earlier test run.
            //Delete all directory content to ensure deterministic test results.
            let _ = fs::remove_dir_all(&dir).unwrap();
            let _ = fs::create_dir(&dir).unwrap();
        }

        dir.push("group1"); // /tmp/bcast_groups/group1
        if !dir.exists() {
            let _ = fs::create_dir(&dir).unwrap();
        }

        dir.pop();
        dir.push("group2"); // /tmp/bcast_groups/group1
        if !dir.exists() {
            let _ = fs::create_dir(&dir).unwrap();
        }

        dir.pop();
        dir.push("group3"); // /tmp/bcast_groups/group1
        if !dir.exists() {
            let _ = fs::create_dir(&dir).unwrap();
        }

        //Create address and links for this radio.
        let key_str = create_random_key();
        //Create the pipe
        let pipe = format!("/tmp/scan_bcast_groups/group1/{}.ipc", key_str);
        worker.radios[0].address = format!("ipc://{}", &pipe);
        File::create(&pipe).unwrap();
        //Create link in group 2
        let link = format!("/tmp/scan_bcast_groups/group2/{}.ipc", key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();
        //Create link in group 3
        let link = format!("/tmp/scan_bcast_groups/group3/{}.ipc", key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();

        //Create address and links for radio #2.
        let other_key_str = create_random_key();
        //Create the pipe
        let pipe = format!("/tmp/scan_bcast_groups/group2/{}.ipc", other_key_str);
        File::create(&pipe).unwrap();
        //Create link in group 2
        let link = format!("/tmp/scan_bcast_groups/group3/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();
        //Create link in group 3
        let link = format!("/tmp/scan_bcast_groups/group1/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();

        //Create address and links for radio #3.
        let other_key_str = create_random_key();
        //Create the pipe
        let pipe = format!("/tmp/scan_bcast_groups/group2/{}.ipc", other_key_str);
        File::create(&pipe).unwrap();
        //Create link in group 2
        let link = format!("/tmp/scan_bcast_groups/group3/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();
        //Create link in group 3
        let link = format!("/tmp/scan_bcast_groups/group1/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();

        //Create address and links for radio #4.
        let other_key_str = create_random_key();
        //Create the pipe
        let pipe = format!("/tmp/scan_bcast_groups/group3/{}.ipc", other_key_str);
        File::create(&pipe).unwrap();
        //Create link in group 2
        let link = format!("/tmp/scan_bcast_groups/group1/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();
        //Create link in group 3
        let link = format!("/tmp/scan_bcast_groups/group2/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();

        //Create address and links for radio #5.
        let other_key_str = create_random_key();
        //Create the pipe
        let pipe = format!("/tmp/scan_bcast_groups/group1/{}.ipc", other_key_str);
        File::create(&pipe).unwrap();
        //Create link in group 2
        let link = format!("/tmp/scan_bcast_groups/group2/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();
        //Create link in group 3
        let link = format!("/tmp/scan_bcast_groups/group3/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();

        //Scan for peers. Should find 4 peers in total.
        let peers : HashSet<Peer> = worker.scan_for_peers(&worker.radios[0]).unwrap();

        assert_eq!(peers.len(), 4);
    }
    */