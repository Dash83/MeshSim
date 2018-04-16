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

use std::iter;
use std::io::{Read, Write};
use self::rand::{Rng, SeedableRng, StdRng};
use self::rustc_serialize::hex::*;
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;
use std::error;
use std::fmt;
use std::io;
use std::fs::{File, self};
use std::str::FromStr;
use std;
use self::byteorder::{NativeEndian, WriteBytesExt};
use std::path::Path;
use std::collections::HashSet;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::net::{TcpListener, TcpStream};
use std::os::unix::net::{UnixStream, UnixListener};
use worker::protocols::*;

//Sub-modules declaration
///Modules that defines the functionality for the test specification.
pub mod worker_config;
pub mod protocols;

// *****************************
// ********** Globals **********
// *****************************
const DNS_SERVICE_NAME : &'static str = "meshsim";
const DNS_SERVICE_TYPE : &'static str = "_http._tcp";
const DNS_SERVICE_PORT : u16 = 23456;
const SIMULATED_SCAN_DIR : &'static str = "bcgroups";
const GOSSIP_FACTOR : f32 = 0.25; //25% of the peer list.

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
    ///Error while performing IO operations.
    IO(io::Error),
    ///Error configuring the worker.
    Configuration(String),

}

impl fmt::Display for WorkerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            WorkerError::Serialization(ref err) => write!(f, "Serialization error: {}", err),
            WorkerError::IO(ref err) => write!(f, "IO error: {}", err),
            WorkerError::Configuration(ref err) => write!(f, "Configuration error: {}", err),            
        }
    }

}

impl error::Error for WorkerError {
    fn description(&self) -> &str {
        match *self {
            WorkerError::Serialization(ref err) => err.description(),
            WorkerError::IO(ref err) => err.description(),
            WorkerError::Configuration(ref err) => err.as_str(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            WorkerError::Serialization(ref err) => Some(err),
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

impl From<io::Error> for WorkerError {
    fn from(err : io::Error) -> WorkerError {
        WorkerError::IO(err)
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
    pub public_key: String, 
    /// Friendly name of the peer. 
    pub name : String,
    ///Endpoint at which the peer is listening for messages.
    pub address : String,
    ///Type of peer.
    pub address_type : OperationMode,
}

impl Peer {
    ///Empty constructor for the Peer struct.
    pub fn new() -> Peer {
        Peer{   public_key : String::from(""),
                name : String::from(""),
                address : String::from(""), 
                address_type : OperationMode::Simulated, }
    }
}

/// Generic struct used to interface between the Worker and the protocols.
/// The sender and destination fields are used in the same way across all message-types and protocols.
/// The payload field encodes the specific data for the particular message type that only the protocol
/// knows about
#[derive(Debug, Serialize, Deserialize)]
pub struct MessageHeader {
    sender : Peer,
    destination : Peer,
    payload : Vec<u8>,
}

impl MessageHeader {
    fn from_vec(data : Vec<u8>) -> Result<MessageHeader, serde_cbor::Error> {
        let msg : Result<MessageHeader, _> = from_reader(&data[..]);
        msg
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

/// Represents a radio used by the worker to send a message to the network.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Radio {
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
    ///Name of the network interface that maps to this Radio object.
    pub radio_name : String,
}

impl Radio {
    /// Send a Worker::Message over the address implemented by the current Radio.
    pub fn send(&self, msg : MessageHeader) -> Result<(), WorkerError> {
        //should the message be sent?

        //should the message be delayed?

        match &msg.destination.address_type {
            &OperationMode::Simulated => self.send_simulated_mode(msg) ,
            &OperationMode::Device => self.send_device_mode(msg),
        }
    }

    fn send_simulated_mode(&self, msg : MessageHeader) -> Result<(), WorkerError> {
        let mut socket = try!(UnixStream::connect(&msg.destination.address));
      
        //info!("Sending message to {}, address {}.", destination.name, destination.address);
        let data = try!(to_vec(&msg));
        try!(socket.write_all(&data));
        info!("Message sent successfully.");
        Ok(())
    }

    fn send_device_mode(&self, msg : MessageHeader) -> Result<(), WorkerError> {
        let mut socket = try!(TcpStream::connect(&msg.destination.address));
        
        //info!("Sending message to {}, address {}.", destination.name, destination.address);
        let data = try!(to_vec(&msg));
        try!(socket.write_all(&data));
        info!("Message sent successfully.");
        Ok(())
    }

    /// Constructor for new Radios
    pub fn new() -> Radio {
        Radio{ delay : 0,
               reliability : 1.0,
               broadcast_groups : vec![],
               radio_name : String::from("") }
    }

    ///Function for adding broadcast groups in simulated mode
    pub fn add_bcast_group(&mut self, group: String) {
        self.broadcast_groups.push(group);
    }

    ///Publishes the service using the mDNS protocol so other devices can discover it.
    pub fn publish_service(&self, service : ServiceRecord) -> Result<(), WorkerError> {
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

    ///Get the public address of the OS-NIC that maps to this Radio object.
    ///It will return the first IPv4 address from a NIC that exactly matches the name.
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
        Err(WorkerError::Configuration(String::from("Network interface specified in configuration not be found.")))
    }
}

/// Worker struct.
/// Main struct for the worker module. Must be created via the ::new(Vec<Param>) method.
/// 
#[derive(Debug, Clone)]
pub struct Worker {
    /// List of radios the worker uses for wireless communication.
    pub radios : Vec<Radio>,
    /// The known neighbors of this worker.
    pub nearby_peers : HashSet<Peer>,
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
    ///The list of all known active members of the network. They might not all be in broadcast
    ///range of this worker.
    pub global_peer_list : Arc<Mutex<HashSet<Peer>>>, //Membership protocol structure.
    ///The list of peers suspected to be dead/out of range.
    suspected_list : Arc<Mutex<Vec<Peer>>>,
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
        info!("Finished initializing.");

        //Get a listener.
        //At this point the network address is bound, so network messages should be cached until we read them.
        let listener = try!(self.get_listener());

        //Get the protocol object.
        let prot_handler = build_protocol_handler(Protocols::TMembership);

        //Do protocol initialization
        //TODO: move this to another thread.
        //Next, join the network
        //self.nearby_peers = try!(self.scan_for_peers(&(self.radios[0])));
        //info!("Found {} peers!", self.nearby_peers.len());
        //self.send_join_message();

        //Start listening for messages
        //TODO: move this to another thread.
        let _ = try!(self.start_listener(listener, &prot_handler));

        //TODO: Start required timers on their own threads.

        //TODO: Wait for all threads.
        Ok(())
    }

    /// Default constructor for Worker strucutre. Assigns it the name Default_Worker
    /// and an empty radio vector.
    pub fn new() -> Worker {
        //Vector of 32 bytes set to 0
        let key : Vec<u8>= iter::repeat(0u8).take(16).collect();
        //Fill the key bytes with random generated numbers
        //let mut gen = OsRng::new().expect("Failed to get OS random generator");
        //gen.fill_bytes(&mut key[..]);
        let radio = Radio::new();
        let mut me = Peer::new();
        me.public_key = key.to_hex().to_string();
        //Global peer list
        let ps : HashSet<Peer> = HashSet::new();
        let gpl = Arc::new(Mutex::new(ps));
        //Suspected peer list
        let sp : Vec<Peer> = Vec::new();
        let sl = Arc::new(Mutex::new(sp));
        Worker{ radios: vec![radio], 
                nearby_peers: HashSet::new(), 
                me: me,
                operation_mode : OperationMode::Simulated,
                random_seed : 0u32,
                scan_interval : 1000u32,
                work_dir : String::new(),
                global_peer_list : gpl,
                suspected_list : sl }
    }

    ///Function for scanning for nearby peers. Scanning method depends on Operation_Mode.
    pub fn scan_for_peers(&self, radio : &Radio) -> Result<HashSet<Peer>, WorkerError> {
        match self.operation_mode {
            OperationMode::Simulated => { 
                //debug!("Scanning for peers in simulated_mode.");
                self.simulated_scan_for_peers(&radio)
            },
            OperationMode::Device => {
                //debug!("Scanning for peers in device_mode.");
                self.device_scan_for_peers(&radio)
            },
        }
    }

    fn device_scan_for_peers(&self, radio : &Radio) -> Result<HashSet<Peer>, WorkerError> {
        let mut peers = HashSet::new();

        //Constructing the external process call
        let mut command = Command::new("avahi-browse");
        command.arg("-r");
        command.arg("-p");
        command.arg("-t");
        command.arg("-l");
        command.arg("_http._tcp");

        //Starting the worker process
        let mut child = try!(command.stdout(Stdio::piped()).spawn());
        let exit_status = child.wait().unwrap();

        if exit_status.success() {
            let mut buffer = String::new();
            let mut output = child.stdout.unwrap();
            output.read_to_string(&mut buffer)?;

            for l in buffer.lines() {
                let tokens : Vec<&str> = l.split(';').collect();
                if tokens.len() > 6 {
                    let serv = ServiceRecord{ service_name : String::from(tokens[3]),
                                            service_type: String::from(tokens[4]), 
                                            host_name : String::from(tokens[6]), 
                                            address : String::from(tokens[7]), 
                                            address_type : String::from(tokens[2]), 
                                            port : u16::from_str_radix(tokens[8], 10).unwrap(),
                                            txt_records : Vec::new() };
                    
                    if serv.service_name.starts_with(DNS_SERVICE_NAME) {
                        //Found a Peer
                        let mut p = Peer::new();
                        //TODO: Deconstruct these Options in a classier way. If not, might as well return emptry string on failure.
                        p.public_key = serv.get_txt_record("PUBLIC_KEY").unwrap_or(String::from("(NO_KEY)"));
                        p.name = serv.get_txt_record("NAME").unwrap_or(String::from("(NO_NAME)"));
                        p.address = format!("{}:{}", serv.address, DNS_SERVICE_PORT);
                        p.address_type = OperationMode::Device;
                        //p.service_record = serv;
                        info!("Found peer {}, address {}", p.name, p.address);
                        peers.insert(p);
                    }
                }
            }
        }
        Ok(peers)
    }

    fn simulated_scan_for_peers(&self, radio : &Radio) -> Result<HashSet<Peer>, WorkerError> {
        let mut peers = HashSet::new();

        //Obtain parent directory of broadcast groups
        let mut parent_dir = Path::new(&self.me.address).to_path_buf();
        let _ = parent_dir.pop(); //bcast group for main address
        let _ = parent_dir.pop(); //bcast group parent directory

        info!("Scanning for nearby peers...");
        debug!("Scanning in dir {}", parent_dir.display());
        for group in &radio.broadcast_groups {
            let dir = format!("{}{}{}", parent_dir.display(), std::path::MAIN_SEPARATOR, group);
            if Path::new(&dir).is_dir() {
                for path in fs::read_dir(dir)? {
                    let peer_file = try!(path);
                    let peer_file = peer_file.path();
                    let peer_file = peer_file.to_str().unwrap_or("");
                    let peer_key = extract_address_key(&peer_file);
                    if !peer_key.is_empty() && peer_key != self.me.public_key {
                        info!("Found {}!", &peer_key);
                        let address = format!("{}", peer_file);
                        let peer = Peer{name : String::from(""), 
                                        public_key : String::from(peer_key), 
                                        address : address,
                                        address_type : OperationMode::Simulated };
                        peers.insert(peer);
                    }
                }
            }
        }
        //Remove self from peers
        //let own_key = Path::new(&self.address).file_name().unwrap();
        //let own_key = own_key.to_str().unwrap_or("").to_string();
        //peers.remove(&own_key);
        Ok(peers)
    }

    fn init(&mut self) -> Result<(), WorkerError> {
        //Create the key-pair for the worker.
        //For now, just filling with random 32 bytes.
        let mut random_bytes = vec![];
        let _ = random_bytes.write_u32::<NativeEndian>(self.random_seed);
        let randomness : Vec<usize> = random_bytes.iter().map(|v| *v as usize).collect();
        let mut gen = StdRng::from_seed(randomness.as_slice());
        let mut key = self.me.public_key.from_hex().unwrap();
        gen.fill_bytes(&mut key[..]);
        self.me.public_key = key.to_hex().to_string();

        //Make sure the required directories are there and writable or create them.
        //check log dir is there
        let mut dir = try!(std::fs::canonicalize(&self.work_dir));
        //debug!("Work dir: {}", dir.display());
        dir.push("log"); //Dir is work_dir/log
        if !dir.exists() {
            //Create log dir
            try!(std::fs::create_dir(dir.as_path()));
            //info!("Created dir file {} ", dir.as_path().display());
        }
        dir.pop(); //Dir is work_dir

        if self.operation_mode == OperationMode::Simulated {
            //Set my own address
            //self.me.address = format!("{}/{}/{}.socket", self.work_dir, SIMULATED_SCAN_DIR, self.me.public_key);

            //check bcast_groups dir is there
            dir.push(SIMULATED_SCAN_DIR); //Dir is work_dir/$SIMULATED_SCAN_DIR
            if !dir.exists() {
                //Create bcast_groups dir
                try!(std::fs::create_dir(dir.as_path()));
                //info!("Created dir file {} ", dir.as_path().display());
            }

            //check current group dir is there
            let mut main_address = String::new();
            let groups = self.radios[0].broadcast_groups.clone();
            for group in groups.iter() {
                dir.push(&group); //Dir is work_dir/$SIMULATED_SCAN_DIR/&group
                
                //Does broadcast group exist?
                if !dir.exists() {
                    //Create group dir
                    try!(std::fs::create_dir(dir.as_path()));
                    //info!("Created dir file {} ", dir.as_path().display());
                }

                //Create address or symlink for this worker
                if main_address.is_empty() {
                    main_address = format!("{}/{}.socket", dir.as_path().display(), self.me.public_key);
                    if Path::new(&main_address).exists() {
                        //Pipe already exists.
                        try!(fs::remove_file(&main_address));
                    }
                    let _ = try!(File::create(&main_address));
                    //debug!("Pipe file {} created.", &main_address);
                    //debug!("Radio address set to {}.", &main_address);
                    self.me.address = format!("{}", main_address);
                } else {
                    let linked_address = format!("{}/{}.socket", dir.as_path().display(), self.me.public_key);
                    if Path::new(&linked_address).exists() {
                        //Pipe already exists
                        dir.pop(); //Dir is work_dir/$SIMULATED_SCAN_DIR
                        continue;
                    }
                    let _ = try!(std::os::unix::fs::symlink(&main_address, &linked_address));
                    //debug!("Pipe file {} created.", &linked_address);
                }
                
                dir.pop(); //Dir is work_dir/$SIMULATED_SCAN_DIR

            }
            //Now, remove the main address socket file. This will create broken symlinks in the broadcast
            //groups that this worker belongs to beyond the first one, but it's needed for the UnixListener
            //type that the file doesn't exist before starting to listen on it.
            //If the operation fails, we should error-out since the listener won't be able to start.
            let _ = fs::remove_file(main_address)?;

        }

        Ok(())
    }

    fn handle_client_simulated(&mut self, mut client_socket : UnixStream, protocol : &Box<Protocol>) -> Result<(), WorkerError> { 
        //Read the data from the unix socket
        let mut data = Vec::new();
        let _bytes_read = try!(client_socket.read_to_end(&mut data));

        //Try to decode the data into a message.
        //let msg_type : Result<MessageType, _> = from_reader(&data[..]);
        //let msg_type = try!(msg_type);
        //self.handle_message(msg_type)
        let msg = try!(MessageHeader::from_vec(data));
        let response = protocol.handle_message(msg);
        match response {
            Some(msg) => { 
                self.radios[0].send(msg)
            },
            None => {
                Ok(())
            }
        }
    }

    fn handle_client_device(&mut self, mut client_socket : TcpStream, protocol : &Box<Protocol>) -> Result<(), WorkerError>  {
        //Read the data from the NIC
        let mut data = Vec::new();
        let _bytes_read = try!(client_socket.read_to_end(&mut data));

        //Try to decode the data into a message.
        //let msg_type : Result<MessageType, _> = from_reader(&data[..]);
        //let msg_type = try!(msg_type);
        //self.handle_message(msg_type)
        let msg = try!(MessageHeader::from_vec(data));
        let response = protocol.handle_message(msg);
        match response {
            Some(msg) => { 
                self.radios[0].send(msg)
            },
            None => {
                Ok(())
            }
        }
    }

    fn start_listener(&mut self, listener : ListenerType, protocol : &Box<Protocol>) -> Result<(), WorkerError> {
        match listener {
            ListenerType::Simulated(l) => {
                self.start_listener_simulated(l, protocol)
            },
            ListenerType::Device(l) => {
                self.start_listener_device(l, protocol)
            },
        }
    }

    fn start_listener_simulated(&mut self, listener : UnixListener, protocol : &Box<Protocol>) -> Result<(), WorkerError> {
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

    fn start_listener_device(&mut self, listener : TcpListener, protocol : &Box<Protocol>) -> Result<(), WorkerError> {
        //Advertise the service to be discoverable by peers before we start listening for messages.
        let mut service = ServiceRecord::new();
        service.service_name = format!("{}_{}", DNS_SERVICE_NAME, self.me.name);
        service.service_type = String::from(DNS_SERVICE_TYPE);
        service.port = DNS_SERVICE_PORT;
        service.txt_records.push(format!("PUBLIC_KEY={}", self.me.public_key));
        service.txt_records.push(format!("NAME={}", self.me.name));
        try!(self.radios[0].publish_service(service));

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

    fn get_listener(&self) -> Result<ListenerType, WorkerError> {
        match self.operation_mode {
            OperationMode::Simulated => {
                let listener = try!(UnixListener::bind(&self.me.address));
                Ok(ListenerType::Simulated(listener))
            },
            OperationMode::Device => {
                let listener = try!(TcpListener::bind(&self.me.address));
                Ok(ListenerType::Device(listener))
            },
        }
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

    //**** Message unit tests ****


    //**** Peer unit tests ****
    

    //Unit test for: create_default_conf_file
    // #[test]
    // fn test_create_default_conf_file() {
    //     let file_path = create_default_conf_file().unwrap();
    //     let md = fs::metadata(file_path).unwrap();
    //     assert!(md.is_file());
    //     assert!(md.len() > 0);
    // }
    
    //**** Worker unit tests ****
    //Unit test for: Worker::new
    #[test]
    fn test_worker_new() {
        let w = Worker::new();
        let w_display = "Worker { radios: [Radio { delay: 0, reliability: 1, broadcast_groups: [], radio_name: \"\" }], nearby_peers: {}, me: Peer { public_key: \"00000000000000000000000000000000\", name: \"\", address: \"\", address_type: Simulated }, work_dir: \"\", random_seed: 0, operation_mode: Simulated, scan_interval: 1000, global_peer_list: Mutex { data: {} }, suspected_list: Mutex { data: [] } }";
        assert_eq!(format!("{:?}", w), String::from(w_display));
    }

    //Unit test for: Worker::init
    #[test]
    fn test_worker_init() {
        let mut config = WorkerConfig::new();
        config.work_dir = String::from("/tmp");
        let mut w = config.create_worker();
        w.radios[0].broadcast_groups.push(String::from("group2"));
        let res = w.init();

        //Check result is not error
        assert!(res.is_ok());
    }

    //Unit test for: Worker::handle_message
    //At this point, I don't know how to test this function. The function uses network connections to communciate to another process,
    //so I don't know how to mock that out in rust.
    /*
    #[test]
    fn test_worker_handle_message() {
        unimplemented!();
    }
    */

    //Unit test for: Worker::join_network
    //At this point, I don't know how to test this function. The function uses network connections to communciate to another process,
    //so I don't know how to mock that out in rust.
    /*
    #[test]
    fn test_worker_send_join_message() {
        unimplemented!();
    }
    */

    //Unit test for: Worker::process_join_message
    //At this point, I don't know how to test this function. The function uses network connections to communciate to another process,
    //so I don't know how to mock that out in rust.
    /*
    #[test]
    fn test_worker_process_join_message() {
        unimplemented!();
    }
    */

    //**** Radio unit tests ****
    //Unit test for: Radio::send
    //At this point, I don't know how to test this function. The function uses network connections to communciate to another process,
    //so I don't know how to mock that out in rust.
    /*
    #[test]
    fn test_radio_send() {
        unimplemented!();
    }
    */

    //Unit test for: Radio::new
    #[test]
    fn test_radio_new() {
        let radio = Radio::new();
        let radio_string = "Radio { delay: 0, reliability: 1, broadcast_groups: [], radio_name: \"\" }";

        assert_eq!(format!("{:?}", radio), String::from(radio_string));
    }

    //Unit test for: Radio::add_bcast_group
    #[test]
    fn test_radio_add_bcast_group() {
        let mut radio = Radio::new();
        radio.add_bcast_group(String::from("group1"));

        assert_eq!(radio.broadcast_groups, vec![String::from("group1")]);
    }

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

    //**** ServiceRecord unit tests ****
    //Unit test for get get_txt_record
    #[test]
    fn test_get_txt_record() {
        let mut record = ServiceRecord::new();
        record.txt_records.push(String::from("NAME=Worker1"));

        assert_eq!(String::from("Worker1"), record.get_txt_record("NAME").unwrap());
    }


    //**** Utility functions ****
    //Used for creating keys that can be addresss of other workers.
    fn create_random_key() -> String {
        let mut rng = rand::thread_rng();
        let mut key = [0u8; 32];
        rng.fill_bytes(&mut key[..]);
        key.to_hex().to_string()
    }
}

// *****************************
// ******** End tests **********
// *****************************