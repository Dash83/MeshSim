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

use std::io::{Read, Write};
use self::serde_cbor::de::*;
use std::error;
use std::fmt;
use std::io;
use std::str::FromStr;
use std;
use std::path::Path;
use std::collections::HashSet;
use std::process::Command;
use std::sync::{PoisonError, MutexGuard};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::{UnixStream, UnixListener};
use worker::protocols::*;
use worker::radio::*;
use self::serde_cbor::ser::*;

//Sub-modules declaration
pub mod worker_config;
pub mod protocols;
pub mod radio;

// *****************************
// ********** Globals **********
// *****************************
const DNS_SERVICE_NAME : &'static str = "meshsim";
const DNS_SERVICE_TYPE : &'static str = "_http._tcp";
const DNS_SERVICE_PORT : u16 = 23456;
const SIMULATED_SCAN_DIR : &'static str = "bcgroups";

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

}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            WorkerError::Serialization(ref err) => write!(f, "Serialization error: {}", err),
            WorkerError::IO(ref err) => write!(f, "IO error: {}", err),
            WorkerError::Configuration(ref err) => write!(f, "Configuration error: {}", err),
            WorkerError::Sync(ref err) => write!(f, "Synchronization error: {}", err),          
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
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            WorkerError::Serialization(ref err) => Some(err),
            WorkerError::IO(ref err) => Some(err),
            WorkerError::Configuration(_) => None,
            WorkerError::Sync(_) => None,
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

/// This enum is used to pass around the socket listener for the type of operation of the worker
pub enum ListenerType {
    ///Simulated mode uses an internal UnixListener
    Simulated(UnixListener),
    ///Device mode uses an internal TCPListener
    Device(TcpListener),
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
    ///Endpoint at which the peer is listening for messages.
    pub address : String,
}

impl Peer {
    ///Empty constructor for the Peer struct.
    pub fn new() -> Peer {
        Peer{   id : String::from(""),
                name : String::from(""),
                address : String::from("")}
    }
}

/// Generic struct used to interface between the Worker and the protocols.
/// The sender and destination fields are used in the same way across all message-types and protocols.
/// The payload field encodes the specific data for the particular message type that only the protocol
/// knows about
#[derive(Debug, Serialize, Deserialize)]
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
}

/// Operation modes for the worker.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
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
    ///Name of the current worker.
    name : String,
    ///Unique ID composed of 16 random numbers represented in a Hex String.
    pub id : String,
    /// Short-range radio for the worker.
    short_radio : Option<Box<Radio>>,
    // TODO: Uncomment this line when the 2nd radio feature is enabled.
    // /// Long-range radio for the worker.
    // long_radio : Box<Radio>,
    ///Directory for the worker to operate. Must have RW access to it. Operational files and 
    ///log files will be written here.
    work_dir : String,
    ///Random seed used for all RNG operations.
    random_seed : u32,
    ///Simulated or Device operation.
    operation_mode : OperationMode,
}

impl Worker {

    /// The main function of the worker. The functions performs the following 3 functions:
    /// 1. Starts up all radios.
    /// 2. Joins the network.
    /// 3. It starts to listen for messages of the network on all addresss
    ///    defined by it's radio array. Upon reception of a message, it will react accordingly to the protocol.
    pub fn start(&mut self) -> Result<(), WorkerError> {        
        //Init the worker
        let _ = try!(self.init());

        //Init the radios and get their respective listeners.
        let mut short_radio = self.short_radio.take().unwrap();
        let listener = try!(short_radio.init());

        info!("Worker finished initializing.");

        //Get the protocol object.
        //TODO: Get protocol from configuration file.
        let mut prot_handler = build_protocol_handler(Protocols::TMembership, short_radio);
        //Initialize protocol.
        let _ = try!(prot_handler.init_protocol());

        //Start listening for messages
        let _ = try!(self.start_listener(listener, &mut prot_handler));

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

    ///Publishes the service using the mDNS protocol so other devices can discover it.
    fn publish_service(&self, service : ServiceRecord) -> Result<(), WorkerError> {
        //Constructing the external process call
        let mut command = Command::new("avahi-publish");
        command.arg("-s");
        command.arg(service.service_name);
        command.arg(service.service_type);
        command.arg(service.port.to_string());

        for r in service.txt_records {
            command.arg(format!(r"{}", r));
        }

        info!("Registering service with command: {:?}", command);

        //Starting the worker process
        let child = try!(command.spawn());
        info!("Process {} started.", child.id());

        Ok(())
    }

    fn handle_client_simulated(&mut self, client_socket : UnixStream, protocol : &mut Box<Protocol>) -> Result<(), WorkerError> {
        let mut client = SimulatedClient{ socket : client_socket};

        loop {
            //let mut data = Vec::new(); //TODO: Not sure this is the right thing here. Does the compiler optimize this allocation?
            //Read the data from the unix socket
            //let _bytes_read = try!(client_socket.read_to_end(&mut data));
            //Try to decode the data into a message.
            //let msg = try!(MessageHeader::from_vec(data));
            let msg = try!(client.read_msg());
            let response = match msg {
                Some(m) => { try!(protocol.handle_message(m)) },
                None => None,
            };

            match response {
                Some(resp_msg) => { 
                    //self.short_radio.send(msg)
                    //let resp_data = try!(to_vec(&resp_msg));
                    let _res = try!(client.send_msg(resp_msg));
                },
                None => {
                    //End of protocol sequence.
                    break;
                }
            }
        }
        Ok(())
    }

    fn handle_client_device(&mut self, client_socket : TcpStream, protocol : &mut Box<Protocol>) -> Result<(), WorkerError>  {
        let mut client = DeviceClient{ socket : client_socket};

        loop {
            //let mut data = Vec::new(); //TODO: Not sure this is the right thing here. Does the compiler optimize this allocation?
            //Read the data from the unix socket
            //let _bytes_read = try!(client_socket.read_to_end(&mut data));
            //Try to decode the data into a message.
            //let msg = try!(MessageHeader::from_vec(data));
            let msg = try!(client.read_msg());
            let response = match msg {
                Some(m) => { try!(protocol.handle_message(m)) },
                None => None,
            };

            match response {
                Some(resp_msg) => { 
                    //self.short_radio.send(msg)
                    //let resp_data = try!(to_vec(&resp_msg));
                    let _res = try!(client.send_msg(resp_msg));
                },
                None => {
                    //End of protocol sequence.
                    break;
                }
            }
        }
        Ok(())
    }

    fn start_listener(&mut self, listener : ListenerType, protocol : &mut Box<Protocol>) -> Result<(), WorkerError> {
        match listener {
            ListenerType::Simulated(l) => {
                self.start_listener_simulated(l, protocol)
            },
            ListenerType::Device(l) => {
                self.start_listener_device(l, protocol)
            },
        }
    }

    fn start_listener_simulated(&mut self, listener : UnixListener, protocol : &mut Box<Protocol>) -> Result<(), WorkerError> {
        //No need for service advertisement in simulated mode.
        //Now listen for messages
        info!("Listening for messages.");
        for stream in listener.incoming() {
            match stream {
                Ok(client) => { 
                    let _result = try!(self.handle_client_simulated(client, protocol));
                },
                Err(e) => { 
                    warn!("Failed to connect to incoming client. Error: {}", e);
                },
            }
        }
        Ok(())
    }

    fn start_listener_device(&mut self, listener : TcpListener, protocol : &mut Box<Protocol>) -> Result<(), WorkerError> {
        //Advertise the service to be discoverable by peers before we start listening for messages.
        let mut service = ServiceRecord::new();
        service.service_name = format!("{}_{}", DNS_SERVICE_NAME, self.name);
        service.service_type = String::from(DNS_SERVICE_TYPE);
        service.port = DNS_SERVICE_PORT;
        service.txt_records.push(format!("PUBLIC_KEY={}", self.id));
        service.txt_records.push(format!("NAME={}", self.name));
        try!(self.publish_service(service));

        //Now listen for messages
        info!("Listening for messages.");
        for stream in listener.incoming() {
            match stream {
                Ok(client) => { 
                    let _result = try!(self.handle_client_device(client, protocol));
                },
                Err(e) => { 
                    warn!("Failed to connect to incoming client. Error: {}", e);
                },
            }
        }
        Ok(())
    }

    // fn get_listener(&self, radio : &Box<Radio>) -> Result<ListenerType, WorkerError> {
    //     match self.operation_mode {
    //         OperationMode::Simulated => {
    //             let listener = try!(UnixListener::bind(&radio.get_self_peer().address));
    //             Ok(ListenerType::Simulated(listener))
    //         },
    //         OperationMode::Device => {
    //             let listener = try!(TcpListener::bind(&radio.get_self_peer().address));
    //             Ok(ListenerType::Device(listener))
    //         },
    //     }
    // }
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
    use worker::worker_config::*;

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