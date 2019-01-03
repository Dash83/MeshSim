//! Mesh simulator Worker module
//! This module defines the Worker struct, which represents one of the nodes 
//! in the Mesh deployment.
//! The worker process has the following responsibilities:
//!   1. Receive the test run-time parameters.
//!   2. Initiliazed all the specified radios with the determined address.
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
extern crate rustc_serialize;
extern crate serde_cbor;
extern crate serde;
extern crate byteorder;
extern crate pnet;
extern crate ipnetwork;
extern crate md5;
extern crate rusqlite;

use std::io::Write;
use self::serde_cbor::de::*;
use std::error;
use std::fmt;
use std::io;
use std::str::FromStr;
use std;
use std::path::Path;
use std::collections::{HashSet, HashMap};
use std::process::{Command, Child};
use std::sync::{PoisonError, MutexGuard};
use worker::protocols::*;
use worker::radio::*;
use self::serde_cbor::ser::*;
use std::sync::{Mutex, Arc};
use self::rand::{StdRng, SeedableRng};
use self::byteorder::{NativeEndian, WriteBytesExt};
use std::thread::{self, JoinHandle};
use self::md5::Digest;
use worker::mobility::*;

//Sub-modules declaration
pub mod worker_config;
pub mod protocols;
pub mod radio;
pub mod listener;
pub mod commands;
pub mod mobility;

// *****************************
// ********** Globals **********
// *****************************
const DNS_SERVICE_NAME : &'static str = "meshsim";
const DNS_SERVICE_TYPE : &'static str = "_http._tcp";
const DNS_SERVICE_PORT : u16 = 23456;

// *****************************
// ********** Structs **********
// *****************************

/// Error type for all possible errors generated in the Worker module.
#[derive(Debug)]
pub enum WorkerError {
    ///Error while serializing data with Serde.
    Serialization(serde_cbor::Error),
    ///Error while performing IO operations.
    IO(io::Error),
    ///Error configuring the worker.
    Configuration(String),
    ///Error in concurrency operations.
    Sync(String),
    ///Error producing a command for this worker
    Command(String),
    ///Error performing DB operations
    DB(rusqlite::Error),
}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            WorkerError::Serialization(ref err) => write!(f, "Serialization error: {}", err),
            WorkerError::IO(ref err) => write!(f, "IO error: {}", err),
            WorkerError::Configuration(ref err) => write!(f, "Configuration error: {}", err),
            WorkerError::Sync(ref err) => write!(f, "Synchronization error: {}", err),   
            WorkerError::Command(ref err) => write!(f, "Command error: {}", err),
            WorkerError::DB(ref err) => write!(f, "Command error: {}", err),
        }
    }

}

impl error::Error for WorkerError {
    fn description(&self) -> &str {
        match *self {
            WorkerError::Serialization(ref err) => err.description(),
            WorkerError::IO(ref err) => err.description(),
            WorkerError::Configuration(ref err) => err.as_str(),
            WorkerError::Sync(ref err) => err.as_str(),
            WorkerError::Command(ref err) => err.as_str(),
            WorkerError::DB(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            WorkerError::Serialization(ref err) => Some(err),
            WorkerError::IO(ref err) => Some(err),
            WorkerError::Configuration(_) => None,
            WorkerError::Sync(_) => None,
            WorkerError::Command(_) => None,
            WorkerError::DB(ref err) => Some(err),
        }
    }
}

impl From<serde_cbor::Error> for WorkerError {
    fn from(err : serde_cbor::Error) -> WorkerError {
        WorkerError::Serialization(err)
    }
}

impl From<io::Error> for WorkerError {
    fn from(err : io::Error) -> WorkerError {
        WorkerError::IO(err)
    }
}

impl<'a> From<PoisonError<MutexGuard<'a, HashSet<Peer>>>> for WorkerError {
    fn from(err : PoisonError<MutexGuard<'a, HashSet<Peer>>>) -> WorkerError {
        WorkerError::Sync(err.to_string())
    }
}

impl<'a> From<PoisonError<MutexGuard<'a, HashMap<String, Peer>>>> for WorkerError {
    fn from(err : PoisonError<MutexGuard<'a, HashMap<String, Peer>>>) -> WorkerError {
        WorkerError::Sync(err.to_string())
    }
}

impl From<rusqlite::Error> for WorkerError {
    fn from(err : rusqlite::Error) -> WorkerError {
        WorkerError::DB(err)
    }
}

///Enum used to encapsualte the addresses a peer has and tag them by type.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum AddressType {
    ///Short range
    ShortRange(String),
    ///Long range
    LongRange(String),
}

impl AddressType {
    fn get_address(&self ) -> String {
        match &self {
            AddressType::ShortRange(s) => s.clone(),
            AddressType::LongRange(s) => s.clone(),
        }
    }

    fn is_short_range(&self) -> bool {
        match &self {
            AddressType::ShortRange(_s) => true,
            AddressType::LongRange(_s) => false,
        }        
    }
}
/// Peer struct.
/// Defines the public identity of a node in the mesh.
/// 
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Peer {
    /// Public key of the peer. It's considered it's public address.
    pub id: String, 
    /// Friendly name of the peer. 
    pub name : String,
    ///Endpoint at which this worker's short_radio is listening for messages.
    pub short_address : Option<String>,
    ///Endpoint at which this worker's long_radio is listening for messages.
    pub long_address : Option<String>,
    // ///The addesses that this peer is listening at.
    // addresses : Vec<AddressType>,
}

impl Peer {
    ///Empty constructor for the Peer struct.
    pub fn new() -> Peer {
        Peer {  id : String::from(""),
                name : String::from(""),
                short_address : None,
                long_address : None }
    }
}

/// Generic struct used to interface between the Worker and the protocols.
/// The sender and destination fields are used in the same way across all message-types and protocols.
/// The payload field encodes the specific data for the particular message type that only the protocol
/// knows about
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MessageHeader {
    ///Sender of the message
    pub sender : Peer,
    ///Destination of the message
    pub destination : Peer,
    ///Optional, serialized payload of the message. 
    /// It's the responsibility of the underlying protocol to know how to deserialize this payload
    /// into a protocol-specific message.
    pub payload : Option<Vec<u8>>,
}

impl MessageHeader {
    ///Creates a MessageHeader from a serialized vector of bytes.
    pub fn from_vec(data : Vec<u8>) -> Result<MessageHeader, serde_cbor::Error> {
        let msg : Result<MessageHeader, _> = from_reader(&data[..]);
        msg
    }

    ///Create new, empty MessageHeader.
    pub fn new() -> MessageHeader {
        MessageHeader{  sender : Peer::new(), 
                        destination : Peer::new(),
                        payload : None }
    }

    /// Produces the MD5 checksum of this message based on the following data:
    /// Sender name
    /// Destination name
    /// Payload
    /// This is done instead of getting the md5sum of the entire structure for testability purposes
    pub fn get_hdr_hash(&self) -> Result<Digest, WorkerError> {
        let mut data = Vec::new();
        data.append(&mut self.sender.name.clone().into_bytes());
        data.append(&mut self.destination.name.clone().into_bytes());
        data.append(&mut self.payload.clone().unwrap());
        let dig = md5::compute(to_vec(&data)?);
        Ok(dig)
    }
}

///Struct to represent DNS TXT records for advertising the meshsim service and obtain records from peers.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct ServiceRecord {
    ///The service name.
    service_name : String,
    ///The service type.
    service_type : String,
    ///Name of the host advertising the service.
    host_name : String,
    ///Address of the host.
    address : String,
    ///What kind of address? (IPv4/IPv6) 
    address_type : String,
    ///Port in which the service is listening.
    port : u16,
    ///Associated TXT records with this record.
    txt_records : Vec<String>,
}

impl ServiceRecord {
    ///Creates a new empty record
    pub fn new() -> ServiceRecord {
        ServiceRecord{  service_name : String::from(""),
                        service_type: String::from(""), 
                        host_name : String::from(""), 
                        address : String::from(""), 
                        address_type : String::from(""), 
                        port : 0,
                        txt_records : Vec::new() }
    }

    ///Return the TXT record value that matches the provided key. The key and values are separated by the '=' symbol on 
    ///service registration.
    pub fn get_txt_record<'a>(&self, key : &'a str) -> Option<String> {
        for t in &self.txt_records {
            //Check record is in KEY=VALUE form.
            let tokens = t.split('=').collect::<Vec<&str>>();
            if tokens.len() == 2 && tokens[0] == key {
                //Found the request record 
                return Some(String::from(tokens[1]))
            }
        }

        None
    }

    ///Publishes the service using the mDNS protocol so other devices can discover it.
    pub fn publish_service(service : ServiceRecord) -> Result<Child, WorkerError> {
        //Constructing the external process call
        let mut command = Command::new("avahi-publish");
        command.arg("-s");
        command.arg(service.service_name);
        command.arg(service.service_type);
        command.arg(service.port.to_string());

        for r in service.txt_records {
            command.arg(format!(r"{}", r));
        }

        debug!("Registering service with command: {:?}", command);

        //Starting the worker process
        let child = try!(command.spawn());
        info!("Process {} started.", child.id());

        Ok(child)
    }
}

/// Operation modes for the worker.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
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

/// Worker struct.
/// Main struct for the worker module. Must be created via the ::new(Vec<Param>) method.
//#[derive(Debug, Clone)]
#[derive(Debug)]
pub struct Worker {
    ///ID of this worker in the DB
    db_id : i64,
    ///Name of the current worker.
    name : String,
    ///Unique ID composed of 16 random numbers represented in a Hex String.
    pub id : String,
    /// Short-range radio for the worker.
    short_radio : Option<Arc<Radio>>,
    /// Long-range radio for the worker.
    long_radio : Option<Arc<Radio>>,
    ///Directory for the worker to operate. Must have RW access to it. Operational files and 
    ///log files will be written here.
    work_dir : String,
    ///Random number generator used for all RNG operations. 
    rng : Arc<Mutex<StdRng>>,
    ///Random number seed used for the rng of all processes.
    seed : u32,
    ///Simulated or Device operation.
    operation_mode : OperationMode,
    /// The protocol that this Worker should run for this configuration.
    pub protocol : Protocols,
}

impl Worker {

    /// The main function of the worker. The functions performs the following 3 functions:
    /// 1. Starts up all radios.
    /// 2. Joins the network.
    /// 3. It starts to listen for messages of the network on all addresss
    ///    defined by it's radio array. Upon reception of a message, it will react accordingly to the protocol.
    pub fn start(&mut self) -> Result<(), WorkerError> {        
        //Init the worker
        let _res = try!(self.init());

        //Init the radios and get their respective listeners.
        let short_radio = self.short_radio.take();
        let long_radio = self.long_radio.take();
        let mut resources = build_protocol_resources( self.protocol, short_radio, long_radio, self.seed, 
                                                           self.id.clone(), self.name.clone(),)?;
        
        info!("Worker finished initializing.");
        
        //Initialize protocol.
        let _res = try!(resources.handler.init_protocol());

        //Start listening for messages
        let prot_handler = Arc::clone(&resources.handler);
        let mut threads : Vec<JoinHandle<Result<(), WorkerError>>> = resources.radio_channels.drain(..).map(|(rx, tx)| { 
            let prot_handler = Arc::clone(&prot_handler);

            thread::spawn(move || {        
                info!("Listening for messages");
                loop {
                    match rx.read_message() {
                        Some(hdr) => { 
                            let prot = Arc::clone(&prot_handler);
                            let r_type = rx.get_radio_range();
                            let tx_channel = Arc::clone(&tx);

                            let _handle = thread::spawn(move || -> Result<(), WorkerError> {
                                let response = prot.handle_message(hdr, r_type)?;

                                match response {
                                    Some(r) => { 
                                        tx_channel.broadcast(r)?;
                                    },
                                    None => { }
                                }
                                   
                                Ok(())
                            });
                        },
                        None => { 
                            warn!("Failed to read incoming message.");
                        }
                    }
                }
            })
        }).collect();

        let com_loop_thread = self.start_command_loop_thread(Arc::clone(&prot_handler))?;
        threads.push(com_loop_thread);

        // let _exit_values = threads.map(|x| x.map(|t| t.join())); //This compact version does not work. It seems the lazy iterator is not evaluated.
        for x in threads {
            // if let Some(h) = x {
            //     debug!("Waiting for a listener thread...");
                let _res = x.join();
            // }
        }

        Ok(())
    }

    fn init(&mut self) -> Result<(), WorkerError> {
        //Make sure the required directories are there and writable or create them.
        //check log dir is there
        let mut dir = try!(std::fs::canonicalize(&self.work_dir));
        dir.push("log"); //Dir is work_dir/log
        if !dir.exists() {
            //Create log dir
            try!(std::fs::create_dir(dir.as_path()));
            info!("Created dir {} ", dir.as_path().display());
        }
        dir.pop(); //Dir is work_dir

        Ok(())
    }

    ///Returns a random number generator seeded with the passed parameter.
    pub fn rng_from_seed(seed : u32) -> StdRng {
        //Create RNG from the provided random seed.
        let mut random_bytes = vec![];
        let _ = random_bytes.write_u32::<NativeEndian>(seed);
        let randomness : Vec<usize> = random_bytes.iter().map(|v| *v as usize).collect();
        StdRng::from_seed(randomness.as_slice())
    }

    fn start_command_loop_thread(&self, protocol_handler : Arc<Protocol>) -> io::Result<JoinHandle<Result<(), WorkerError>>> {
        let tb = thread::Builder::new();
        tb.name(String::from("CommandLoop"))
        .spawn(move || { 
            let mut input = String::new();
            debug!("Command loop started");
            loop {
                match io::stdin().read_line(&mut input) {
                    Ok(_bytes) => {
                        match input.parse::<commands::Commands>() {
                            Ok(command) => {
                                info!("Command received: {:?}", &command);
                                match Worker::process_command(command, Arc::clone(&protocol_handler)) {
                                    Ok(_) => { /* All good! */ },
                                    Err(e) => {
                                        error!("Error executing command: {}", e);
                                    }
                                }
                            },
                            Err(e) => { 
                                error!("Error parsing command: {}", e);
                            },
                        }
                    }
                    Err(error) => { 
                        error!("{}", error);
                    }
                }
            }
            Ok(())
        })
    }

    fn process_command(com : commands::Commands, ph : Arc<Protocol>) -> Result<(), WorkerError> {
        match com {
            commands::Commands::Add_bcg(radio, group) => { 
                unimplemented!("Adding broadcast groups is not yet supported.");
            },
            commands::Commands::Rem_bcg(radio, group) => { 
                unimplemented!("Removing broadcast groups is not yet supported.");
            },
            commands::Commands::Send(destination, data) => {
                let _res = ph.send(destination, data)?;
            }
        }
        Ok(())
    }
}

// *****************************
// ******* End structs *********
// *****************************

fn extract_address_key<'a>(address : &'a str) -> String {
    let own_key = Path::new(address).file_stem();
    if own_key.is_none() {
        return String::from("");
    }
    own_key.unwrap().to_str().unwrap_or("").to_string()
}

// *****************************
// ********** Tests ************
// *****************************


#[cfg(test)]
mod tests {
    use super::*;
    //use worker::worker_config::*;

    //**** Peer unit tests ****
    
    
    //**** Worker unit tests ****
    //Unit test for: Worker::new
    // #[test]
    // fn test_worker_new() {
    //     let w = Worker::new();
    //     let w_display = "Worker { short_radio: Radio { delay: 0, reliability: 1, broadcast_groups: [], radio_name: \"\" }, long_radio: Radio { delay: 0, reliability: 1, broadcast_groups: [], radio_name: \"\" }, nearby_peers: {}, me: Peer { public_key: \"00000000000000000000000000000000\", name: \"\", address: \"\", address_type: Simulated }, work_dir: \"\", random_seed: 0, operation_mode: Simulated, scan_interval: 1000, global_peer_list: Mutex { data: {} }, suspected_list: Mutex { data: [] } }";
    //     assert_eq!(format!("{:?}", w), String::from(w_display));
    // }

    // //Unit test for: Worker::init
    // #[test]
    // fn test_worker_init() {
    //     let mut config = WorkerConfig::new();
    //     config.work_dir = String::from("/tmp");
    //     let mut w = config.create_worker();
    //     w.short_radio.broadcast_groups.push(String::from("group2"));
    //     let res = w.init();

    //     //Check result is not error
    //     assert!(res.is_ok());
    // }

    //**** ServiceRecord unit tests ****
    //Unit test for get get_txt_record
    #[test]
    fn test_get_txt_record() {
        let mut record = ServiceRecord::new();
        record.txt_records.push(String::from("NAME=Worker1"));

        assert_eq!(String::from("Worker1"), record.get_txt_record("NAME").unwrap());
    }

}

// *****************************
// ******** End tests **********
// *****************************