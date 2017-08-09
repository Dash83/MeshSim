//! Mesh simulator Worker module
//! This module defines the Worker struct, which represents one of the nodes 
//! in the Mesh deployment.
//! The worker process has the following responsibilities:
//!   1. Receive the test run-time parameters.
//!   2. Initiliazed all the specified radios with the determined endpoint.
//!   3. Introduce itself to the group, wait for response.
//!   4. Keep a list of peers for each radio (and one overall list).
//!   5. Send messages to any of those peers as required.
//!   6. Progagate messages receives from other peers that are not destined to itself.


//#![deny(missing_docs,
//        missing_debug_implementations, missing_copy_implementations,
//        trivial_casts, trivial_numeric_casts,
//        unsafe_code,
//        unstable_features,
//        unused_import_braces, unused_qualifications)]

// Lint options for this module
#![deny(missing_docs,
        trivial_casts, trivial_numeric_casts,
        unsafe_code,
        unstable_features,
        unused_import_braces, unused_qualifications)]

extern crate rand;
extern crate nanomsg;
extern crate rustc_serialize;
extern crate serde_cbor;
extern crate serde;

use std::iter;
use std::io::{Read, Write};
use self::rand::{OsRng, Rng};
use self::nanomsg::{Socket, Protocol};
use self::rustc_serialize::hex::*;
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;
use std::error;
use std::fmt;
use std::io;
use std::fs::{OpenOptions, File, self};
use std::str::FromStr;
use std;

// *****************************
// ********** Traits **********
// *****************************
//trait message<T> {
//    fn get_sender(&self) -> Peer;
//    fn get_recipient(&self) -> Peer;
//    fn get_payload(&self) -> T;
//}


// *****************************
// ******* End traits  *********
// *****************************

// *****************************
// ********** Structs **********
// *****************************

/// Error type for all possible errors generated in the Worker module.
#[derive(Debug)]
pub enum WorkerError {
    ///Error while serializing data with Serde.
    Serialization(serde_cbor::Error),
    ///Error while using the Nanomsg network library.
    Nanomsg(nanomsg::Error),
    ///Error while performing IO operations.
    IO(io::Error),
    ///Error configuring the worker.
    Configuration(String),

}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            WorkerError::Serialization(ref err) => write!(f, "Serialization error: {}", err),
            WorkerError::Nanomsg(ref err) => write!(f, "Network error: {}", err),
            WorkerError::IO(ref err) => write!(f, "IO error: {}", err),
            WorkerError::Configuration(ref err) => write!(f, "Configuration error: {}", err),            
        }
    }

}

impl error::Error for WorkerError {
    fn description(&self) -> &str {
        match *self {
            WorkerError::Serialization(ref err) => err.description(),
            WorkerError::Nanomsg(ref err) => err.description(),
            WorkerError::IO(ref err) => err.description(),
            WorkerError::Configuration(ref err) => err.as_str(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            WorkerError::Serialization(ref err) => Some(err),
            WorkerError::Nanomsg(ref err) => Some(err),
            WorkerError::IO(ref err) => Some(err),
            WorkerError::Configuration(_) => None,
        }
    }
}

impl From<serde_cbor::Error> for WorkerError {
    fn from(err : serde_cbor::Error) -> WorkerError {
        WorkerError::Serialization(err)
    }
}

impl From<nanomsg::Error> for WorkerError {
    fn from(err : nanomsg::Error) -> WorkerError {
        WorkerError::Nanomsg(err)
    }
}

impl From<io::Error> for WorkerError {
    fn from(err : io::Error) -> WorkerError {
        WorkerError::IO(err)
    }
}

/// This enum represents the types of network messages supported in the protocol as well as the
/// data associated with them. For each message type, an associated struct will be created to represent 
/// all the data needed to operate on such message.
#[derive(Debug, Serialize, Deserialize)]
pub enum MessageType {
    ///Message that a peer sends to join the network.
    Join(JoinMessage),
    ///Reply to a JOIN message sent from a current member of the network.
    Ack(AckMessage),
    ///General data message to be sent to a given member of the network.
    Data(DataMessage),
}

/// Peer struct.
/// Defines the public identity of a node in the mesh.
/// 
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Peer {
    /// Public key of the peer. It's considered it's public address.
    pub public_key: String, 
    /// Friendly name of the peer. 
    pub name : String,
}
  
/// Trait that must be implemented for all message types.
pub trait Message {}

/// The type of message passed as payload for Join messages.
/// The actual message is not required at this point, but used for future compatibility.
#[derive(Debug, Serialize, Deserialize)]
pub struct JoinMessage {
    sender: Peer,
}

/// Ack message used to reply to Join messages
#[derive(Debug, Serialize, Deserialize)]
pub struct AckMessage {
    sender: Peer,
    neighbors : Vec<Peer>,
}

/// General purposes data message for the network
#[derive(Debug, Serialize, Deserialize)]
pub struct DataMessage;

/// Represents a radio used by the work to send a message to the network.
#[derive(Debug, Serialize, Deserialize)]
pub struct Radio {
    //socket : whatever socket type
    /// The endpoint used for the socket. In this version of the software,
    /// it's the pipe used for this radio.
    endpoint : String,
    /// delay parameter used by the test. Sets a number of millisecs
    /// as base value for delay of messages. The actual delay for message sending 
    /// will be a a percentage between the constants MESSAGE_DELAY_LOW and MESSAGE_DELAY_HIGH
    delay : u32,
    /// Reliability parameter used by the test. Sets a number of percentage
    /// between (0 and 1] of probability that the message sent by this worker will
    /// not reach its destination.
    reliability : f64,
    ///Broadcast group for this radio. Only used in simulated mode.
    pub broadcast_groups : Vec<String>,
}

/// Operation modes for the worker.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum OperationMode {
    /// Simulated: The worker process is part of a simulated environment, running as one of many processes in the same machine.
    Simulated,
    /// Device: The worker is running on the actual hardware and has access to the radio transmitters.
    Device,
}

impl fmt::Display for OperationMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
        // or, alternatively:
        // fmt::Debug::fmt(self, f)
    }
}

impl FromStr for OperationMode {
    type Err = WorkerError;

    fn from_str(s: &str) -> Result<OperationMode, WorkerError> {
        let u = s.to_uppercase();
        match u.as_str() {
            "SIMULATED" => Ok(OperationMode::Simulated),
            "DEVICE" => Ok(OperationMode::Device),
            _ => Err(WorkerError::Configuration("Unsupported operation mode.".to_string()))
        }
    }
}
//impl Message<MessageType> {
//    pub fn new(mtype: MessageType) -> Message<MessageType> {
//        match mtype {
//            MessageType::Ack => { 
//                Message{sender : Peer{ public_key : "".to_string(), name : "".to_string(), payload:}, 
//                        recipient : Peer{ public_key : "".to_string(), name : "".to_string()}}
//            },
//            MessageType::Data => { },
//            MessageType::Join => { }, 
//        }
//    }
  //  
//}

impl Radio {
    /// Send a Worker::Message over the endpoint implemented by the current Radio.
    pub fn send(&self, msg : MessageType, destination: &Peer) -> Result<(), WorkerError> {
        let mut socket = try!(Socket::new(Protocol::Push));
        let endpoint = format!("ipc:///tmp/{}.ipc", destination.public_key);
        
        try!(socket.connect(&endpoint));
        //info!("Connected to endpoint.");

        info!("Sending message to {:?}.", destination.name);
        let data = try!(to_vec(&msg));
        try!(socket.write_all(&data));
        info!("Message sent successfully.");

        Ok(())
    }

    /// Constructor for new Radios
    pub fn new() -> Radio {
        Radio{ endpoint : String::from(""), 
               delay : 0,
               reliability : 1.0,
               broadcast_groups : vec![] }
    }

    ///Function for adding broadcast groups in simulated mode
    pub fn add_bcast_group(&mut self, group: String) {
        self.broadcast_groups.push(group);
    }

    ///Function for scanning for nearby peers.
    pub fn scan_for_peers(&self) ->  Result<Vec<String>, WorkerError> {
        let mut peers = vec![];
        info!("Scanning for nearby peers...");
        for group in &self.broadcast_groups {
            let paths = fs::read_dir(format!("//tmp/{}", group)).unwrap();
            for path in paths {
                let peer = path.unwrap().file_name();
                let peer_name = peer.to_str().unwrap();
                peers.push(String::from(peer_name));
            }
        }
        
        Ok(peers)
    }
}

/// Worker struct.
/// Main struct for the worker module. Must be created via the ::new(Vec<Param>) method.
/// 
#[derive(Debug, Serialize, Deserialize)]
pub struct Worker {
    /// List of radios the worker uses for wireless communication.
    pub radios : Vec<Radio>,
    /// The known neighbors of this worker.
    pub peers : Vec<Peer>,
    ///Peer object describing this worker.
    pub me : Peer,
    ///Directory for the worker to operate. Must have RW access to it. Operational files and 
    ///log files will be written here.
    work_dir : String,
    ///Random seed used for all RNG operations.
    random_seed : u32,
    ///Simulated or Device operation.
    operation_mode : OperationMode,
    ///How often (ms) should the worker scan for new peers.
    scan_interval : u32,
}

impl Worker {

    /// The main function of the worker. The functions performs the following 3 functions:
    /// 1. Starts up all radios.
    /// 2. Joins the network.
    /// 3. It starts to listen for messages of the network on all endpoints
    ///    defined by it's radio array. Upon reception of a message, it will react accordingly to the protocol.
    pub fn start(&mut self) -> Result<(), WorkerError> {        
        //First, turn on radios.
        //self.start_radios();
        let mut socket = try!(Socket::new(Protocol::Pull));
        let endpoint = &self.radios[0].endpoint.clone();
        try!(socket.bind(&endpoint));
        //info!("Successfully bound to endpoint {}", endpoint);

        //Next, join the network
        let peers : Vec<String> = try!(self.radios[0].scan_for_peers());
        let num = peers.len() - self.radios[0].broadcast_groups.len();
        info!("Found {} peers!", num);
        self.join_network();

        //Now listen for messages
        info!("Listening for messages.");
        loop {
            let mut buffer = Vec::new();
            try!(socket.read_to_end(&mut buffer));
            //info!("Message received");
            try!(self.handle_message(&buffer));
        }

    }

    /// Default constructor for Worker strucutre. Assigns it the name Default_Worker
    /// and an empty radio vector.
    pub fn new() -> Worker {
        //Vector of 32 bytes set to 0
        let mut key : Vec<u8>= iter::repeat(0u8).take(32).collect();
        //Fill the key bytes with random generated numbers
        //let mut gen = OsRng::new().expect("Failed to get OS random generator");
        //gen.fill_bytes(&mut key[..]);
        let mut radio = Radio::new();
        //radio.endpoint = format!("ipc:///tmp/{}.ipc", key.to_hex()).to_string();
        Worker{ radios: vec![radio], 
                peers: Vec::new(), 
                me: Peer{ name : String::new(), public_key : key.to_hex().to_string()},
                operation_mode : OperationMode::Simulated,
                random_seed : 0u32,
                scan_interval : 1000u32,
                work_dir : String::new() }
    }

    //pub fn new() -> Worker {

    //}

    fn handle_message(&mut self, data : &Vec<u8>) -> Result<(), WorkerError> {
        let msg_type : Result<MessageType, _> = from_reader(&data[..]);
        let msg_type = try!(msg_type);

        match msg_type {
            MessageType::Join(msg) => {
                self.process_join_message(msg);
            },
            MessageType::Ack(msg) => {
                 self.process_ack_message(msg);
            },
            MessageType::Data(_) => { },
        }
        Ok(())
    }

    fn join_network(&self) {
        for r in &self.radios {
            //Send messge to each Peer reachable by this radio
            for p in &self.peers {
                let data = JoinMessage { sender : self.me.clone()};
                let msg = MessageType::Join(data);
                let _ = r.send(msg, p);
            }

        }
    }

    // fn start_radios(&mut self) {
    //     //Need to create a socket for each radio and endpoint
    //     //For now, we just use the first radio.
    //     //self.radios[0].socket = Socket::new(Protocol::Pull).unwrap();
    //     let endpoint = &self.radios[0].endpoint.clone();
    //     self.radios[0].socket.bind(&endpoint);
    // }

    fn process_join_message(&mut self, msg : JoinMessage) {
        info!("Received JOIN message from {:?}", msg.sender.name);
        //Respond with ACK 
        //Need to responde through the same radio we used to receive this.
        // For now just use default radio.
        let data = AckMessage{sender: self.me.clone(), neighbors : self.peers.clone()};
        let ack_msg = MessageType::Ack(data);
        let _ = self.radios[0].send(ack_msg, &msg.sender);

        //Add new node to membership list
        self.peers.push(msg.sender.clone());
    }

    fn process_ack_message(&mut self, msg: AckMessage)
    {
        info!("Received ACK message from {:?}", msg.sender.name);
        for p in msg.neighbors {
            // TODO: check the peer doesn't already exist
            info!("Adding peer {:?} to list.", p.name);
            self.peers.push(p);
        }
    }

    /// This method is not parte of the oficial interface. Since the nodes have no way to
    /// discover each other without a shared mediuem (wifi-direct broadcast) they can never
    /// build the mesh while in simulated mode. This method is used to bootstrap the mesh.
    /// All new nodes need at least 1 peer to be introduced into the network.
    pub fn add_peers(&mut self, peers : Vec<Peer>) {
        for p in peers {
            self.peers.push(p);
        }
        
    }

}

impl Message for JoinMessage {
     
}

impl Message for AckMessage {
     
}

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
    
}

impl WorkerConfig {
    ///Creates a new configuration for a Worker with default settings.
    pub fn new() -> WorkerConfig {
        WorkerConfig{worker_name : "worker1".to_string(),
                     work_dir : ".".to_string(),
                     random_seed : 12345, 
                     operation_mode : OperationMode::Simulated,
                     broadcast_groups : Some(vec!("group1".to_string())),
                     reliability : Some(1.0),
                     delay : Some(0),
                     scan_interval : Some(2000)
                    }
    }

    ///Creates a new Worker object configured with the values of this configuration object.
    pub fn create_worker(self) -> Worker {
        let mut obj = Worker::new();
        obj.me.name = self.worker_name;
        obj.work_dir = self.work_dir;
        obj.random_seed = self.random_seed;
        obj.operation_mode = self.operation_mode;
        obj.radios[0].broadcast_groups = self.broadcast_groups.unwrap_or(obj.radios[0].broadcast_groups.clone());
        obj.radios[0].reliability = self.reliability.unwrap_or(obj.radios[0].reliability);
        obj.radios[0].delay = self.delay.unwrap_or(obj.radios[0].delay);
        obj.scan_interval = self.scan_interval.unwrap_or(obj.scan_interval);
    
        obj
    }

    ///Writes the current configuration object to a formatted configuration file, that can be passed to
    ///the worker_cli binary.
    pub fn write_to_file<'a>(&self, dir : &'a str, file_name : &'a str) -> Result<String, WorkerError> {
    //Create configuration file
    let file_path = format!("{}{}{}", dir, std::path::MAIN_SEPARATOR, file_name);
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

    //file.flush().expect("Error flusing toml file to disk.");
    Ok(file_path.to_string())
}
}

// *****************************
// ******* End structs *********
// *****************************


// *****************************
// ********** Tests ************
// *****************************


#[cfg(test)]
mod tests {
    use super::*;

    static endpoint1: &'static str = "ipc:///tmp/endpoint1.ipc";

    //**** Message unit tests ****
    #[ignore]
    #[test]
    fn dummy_test() {
        panic!("test failed!");
    }

    //**** Radio unit tests ****
    //#[test]
    //fn create_empty_radio() {
    //    let r1 = Radio{ endpoint : endpoint1.to_string(), delay : 0, reliability: 1.0 };
    //    let mut r2 = Radio::new();
    //    r2.endpoint = String::from(endpoint1);
    //    assert_eq!(r1.endpoint, r2.endpoint);
    //    assert_eq!(r1.delay, r2.delay);
    //    assert_eq!(r1.reliability, r2.reliability);
    //}

    //**** Peer unit tests ****
    //**** Worker unit tests ****

}

// *****************************
// ******** End tests **********
// *****************************