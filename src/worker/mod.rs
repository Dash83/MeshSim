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

// *****************************
// ********** Globals **********
// *****************************
const DNS_SERVICE_NAME : &'static str = "meshsim";
const DNS_SERVICE_TYPE : &'static str = "_http._tcp";
const DNS_SERVICE_PORT : u16 = 23456;
const SIMULATED_SCAN_DIR : &'static str = "bcast_groups";
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

/// This enum represents the types of network messages supported in the protocol as well as the
/// data associated with them. For each message type, an associated struct will be created to represent 
/// all the data needed to operate on such message.
#[derive(Debug, Serialize, Deserialize)]
pub enum MessageType {
    ///Message that a peer sends to join the network.
    Join(JoinMessage),
    ///Reply to a JOIN message sent from a current member of the network.
    Ack(AckMessage),
    ///Hearbeat message to check on the health of a peer.
    Heartbeat(HeartbeatMessage),
    ///Alive message, which is a response to the heartbeat message.
    Alive(AliveMessage),
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
    global_peer_list : HashSet<Peer>,
}

/// General purposes data message for the network
#[derive(Debug, Serialize, Deserialize)]
pub struct DataMessage;

/// Heartbeat message used to check on the status of nearby peers.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    sender: Peer,
    global_peer_list : HashSet<Peer>,
}

/// Response message to the heartbeat message.
#[derive(Debug, Serialize, Deserialize)]
pub struct AliveMessage {
    sender: Peer,
    global_peer_list : HashSet<Peer>,
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
}

impl Radio {
    /// Send a Worker::Message over the address implemented by the current Radio.
    pub fn send(&self, msg : MessageType, destination: &Peer) -> Result<(), WorkerError> {
        match destination.address_type {
            OperationMode::Simulated => self.send_simulated_mode(msg, destination) ,
            OperationMode::Device => self.send_device_mode(msg, destination),
        }
    }

    fn send_simulated_mode(&self, msg : MessageType, destination: &Peer) -> Result<(), WorkerError> {
        let mut socket = try!(UnixStream::connect(&destination.address));
      
        //info!("Sending message to {}, address {}.", destination.name, destination.address);
        let data = try!(to_vec(&msg));
        try!(socket.write_all(&data));
        info!("Message sent successfully.");
        Ok(())
    }

    fn send_device_mode(&self, msg : MessageType, destination: &Peer) -> Result<(), WorkerError> {
        let mut socket = try!(TcpStream::connect(&destination.address));
        
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
               broadcast_groups : vec![] }
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

        //Do protocol initialization
        //TODO: move this to another thread.
        //Next, join the network
        self.nearby_peers = try!(self.scan_for_peers(&(self.radios[0])));
        info!("Found {} peers!", self.nearby_peers.len());
        self.send_join_message();

        //Start listening for messages
        //TODO: move this to another thread. This should be started before any protocol messages go out.
        let _ = try!(self.start_listener());

        //TODO: Start required timers on their own threads.

        //TODO: Wait for all threads.
        Ok(())
    }

    /// Default constructor for Worker strucutre. Assigns it the name Default_Worker
    /// and an empty radio vector.
    pub fn new() -> Worker {
        //Vector of 32 bytes set to 0
        let key : Vec<u8>= iter::repeat(0u8).take(32).collect();
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

    fn handle_message(&mut self, encapsulated_message : MessageType) -> Result<(), WorkerError> {
        match encapsulated_message {
            MessageType::Join(msg) => {
                self.process_join_message(msg)
            },
            MessageType::Ack(msg) => {
                 self.process_ack_message(msg)
            },
            MessageType::Heartbeat(msg) => {
                self.process_heartbeat_message(msg)
            },
            MessageType::Alive(msg) => {
                self.process_alive_message(msg)
            }
        }
    }
   
    fn handle_client_simulated(&mut self, mut client_socket : UnixStream) -> Result<(), WorkerError> { 
        //Read the data from the unix socket
        let mut data = Vec::new();
        let _bytes_read = try!(client_socket.read_to_end(&mut data));

        //Try to decode the data into a message.
        let msg_type : Result<MessageType, _> = from_reader(&data[..]);
        let msg_type = try!(msg_type);
        self.handle_message(msg_type)
    }

    fn handle_client_device(&mut self, mut client_socket : TcpStream) -> Result<(), WorkerError>  {
        //Read the data from the NIC
        let mut data = Vec::new();
        let _bytes_read = try!(client_socket.read_to_end(&mut data));

        //Try to decode the data into a message.
        let msg_type : Result<MessageType, _> = from_reader(&data[..]);
        let msg_type = try!(msg_type);
        self.handle_message(msg_type)
    }

    /// The first message of the protocol. 
    /// When to send: At start-up, after doing a scan of nearby peers.
    /// Receiver: All Nearby peers.
    /// Payload: Self Peer object.
    /// Actions: None.
    fn send_join_message(&self) {
        for r in &self.radios {
            //Send messge to each Peer reachable by this radio
            for p in &self.nearby_peers {
                info!("Sending join message to {}, address {}", p.name, p.public_key);
                let data = JoinMessage { sender : self.me.clone()};
                let msg = MessageType::Join(data);
                let _ = r.send(msg, p);
            }

        }
    }

    /// A response to a new peer joining the network.
    /// When to send: After receiving a join message.
    /// Sender: A new peer in range joining the network.
    /// Payload: Global peer list.
    /// Actions:
    ///   1. Add sender to global membership list and nearby peer list.
    ///   2. Construct an ACK message and reply with it.
    fn process_join_message(&mut self, msg : JoinMessage) -> Result<(), WorkerError> {
        info!("Received JOIN message from {}, address {}", msg.sender.name, msg.sender.address);
        //Add new node to nearby peer list
        self.nearby_peers.insert(msg.sender.clone());
        //Obtain a reference to our current GPL.
        let gpl = self.global_peer_list.clone();
        //Obtain a lock to the underlying data.
        let mut gpl = gpl.lock().unwrap();
        gpl.insert(msg.sender.clone());

        //Respond with ACK 
        //Need to responde through the same radio we used to receive this.
        // For now just use default radio.
        let data = AckMessage{sender: self.me.clone(), global_peer_list : self.nearby_peers.clone()};
        let ack_msg = MessageType::Ack(data);
        info!("Sending ACK message to {}, address {}", msg.sender.name, msg.sender.address);
        /*
        match self.radios[0].send(ack_msg, &msg.sender) {
            Ok(_) => info!("ACK message sent"),
            Err(e) => error!("ACK message failed to be sent. Error:{}", e),
        };*/
        self.radios[0].send(ack_msg, &msg.sender)
    }

    /// The final part of the protocol's initial handshake
    /// When to send: After receiving an ACK message.
    /// Sender: A peer in the network that received a JOIN message.
    /// Payload: Global peer list.
    /// Actions:
    ///   1. Add the difference between the payload and current GPL to the GPL.
    fn process_ack_message(&mut self, msg: AckMessage) -> Result<(), WorkerError> {
        info!("Received ACK message from {}, address {}", msg.sender.name, msg.sender.address);
        //Obtain a reference to our current GPL.
        let gpl = self.global_peer_list.clone();
        //Obtain a lock to the underlying data.
        let mut gpl = gpl.lock().unwrap();

        //This worker just started, but might be getting an ACK message from many
        //nearby peers. Diff the incoming set with the current set using an outer join.
        let gpl_snapshot = gpl.clone();
        for p in msg.global_peer_list.difference(&gpl_snapshot) {
            info!("Adding peer {}/{} to list.", p.name, p.public_key);
            gpl.insert(p.clone());
        }
        Ok(())
    }

    /// A message that is sent periodically to a given amount of members
    /// of the nearby list to check if they are still alive.
    /// When to send: After the heartbeat timer expires.
    /// Receiver: Random member of the nearby peer list.
    /// Payload: Global peer list.
    /// Actions: 
    ///   1. Add the sent peers to the suspected list.
    fn send_heartbeat_message(&self) {
        //Get the RNG
        let mut random_bytes = vec![];
        let _ = random_bytes.write_u32::<NativeEndian>(self.random_seed);
        let randomness : Vec<usize> = random_bytes.iter().map(|v| *v as usize).collect();
        let mut gen = StdRng::from_seed(randomness.as_slice());

        //Will contact a given amount of current number of nearby peers.
        let num_of_peers = self.nearby_peers.len() as f32 * GOSSIP_FACTOR;
        let num_of_peers = num_of_peers.ceil() as u32;

        //Get a handle and lock on the suspected peer list.
        let spl = self.suspected_list.clone();
        let mut spl = spl.lock().unwrap();

        //Get a handle and lock of the GPL
        let gpl = self.global_peer_list.clone();
        let gpl = gpl.lock().unwrap();

        //Get a copy of the GPL for iteration and selecting the random peers.
        let gpl_iter = gpl.clone();
        let mut gpl_iter = gpl_iter.iter();
        for _i in 0..num_of_peers {
            let selection : usize = gen.next_u32() as usize % self.nearby_peers.len();
            let mut j = 0;
            while j < selection {
                gpl_iter.next();
                j += 1;
            }
            //The chosen peer
            let recipient = gpl_iter.next().unwrap();

            //Add the suspected peer to the suspected list.
            spl.push(recipient.clone());

            let gpl_snapshot = gpl.clone();
            let data = HeartbeatMessage{ sender : self.me.clone(),
                                        global_peer_list : gpl_snapshot};
            let msg = MessageType::Heartbeat(data);
            info!("Sending a Heartbeat message to {}, address {}", recipient.name, recipient.address);
            let _res = self.radios[0].send(msg, recipient);
        }
    }

    /// A response to a heartbeat message.
    /// When to send: After receiving a heartbeat message.
    /// Sender: A nearby peer in range.
    /// Payload: Global peer list.
    /// Actions:
    ///   1. Construct an alive message with this peer's GPL and send it.
    ///   2. Add the difference between the senders GPL and this GPL.
    fn process_heartbeat_message(&self, msg : HeartbeatMessage) -> Result<(), WorkerError> {
        info!("Received Heartbeat message from {}, address {}", msg.sender.name, msg.sender.address);
        //The sender is asking if I'm alive. Before responding, let's take a look at their GPL
        //Obtain a reference to our current GPL.
        let gpl = self.global_peer_list.clone();
        //Obtain a lock to the underlying data.
        let mut gpl = gpl.lock().unwrap();

        //Diff the incoming set with the current set using an outer join.
        let gpl_snapshot = gpl.clone();
        for p in msg.global_peer_list.difference(&gpl_snapshot) {
            info!("Adding peer {}/{} to list.", p.name, p.public_key);
            gpl.insert(p.clone());
        }

        //Now construct and send an alive message to prove we are alive.
        let data = AliveMessage{ sender : self.me.clone(),
                                 global_peer_list : gpl.clone()};
        let alive_msg = MessageType::Alive(data);
        self.radios[0].send(alive_msg, &msg.sender)
    }

    /// The final part of the heartbeat-alive part of the protocol. If this message is 
    /// received, it means a peer to which we sent a heartbeat message is still alive.
    /// Sender: A nearby peer in range that received a heartbeat message from this peer.
    /// Payload: Global peer list.
    /// Actions:
    ///   1. Add the difference between the senders GPL and this GPL.
    ///   2. Remove the sender from the list of suspected dead peers.
    fn process_alive_message(&self, msg : AliveMessage) -> Result<(), WorkerError> {
        info!("Received Alive message from {}, address {}", msg.sender.name, msg.sender.address);
        //Get a handle and lock on the suspected peer list.
        let spl = self.suspected_list.clone();
        let mut spl = spl.lock().unwrap();
        //Find the index of the element to remove.
        let mut index : i32 = -1;
        let mut j = 0;
        let spl_iter = spl.clone();
        let spl_iter = spl_iter.iter();
        for p in spl_iter {
            if *p == msg.sender {
                index = j;
                break; //Found the element
            }
            j += 1;
        }
        //Remove the sender from the suspected list.
        if index > 0 {
            let index = index as usize;
            spl.remove(index);
        } else {
            //Received an alive message from a peer that was not in the list.
            //In that case, ignore and return.
            warn!("Received ALIVE message from {}, address {}, that was not in the spl", msg.sender.name, msg.sender.address);
            return Ok(())
        }

        //Obtain a reference to our current GPL.
        let gpl = self.global_peer_list.clone();
        //Obtain a lock to the underlying data.
        let mut gpl = gpl.lock().unwrap();

        //Diff the incoming set with the current set using an outer join.
        let gpl_snapshot = gpl.clone();
        for p in msg.global_peer_list.difference(&gpl_snapshot) {
            info!("Adding peer {}/{} to list.", p.name, p.public_key);
            gpl.insert(p.clone());
        }
        Ok(())
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
            self.me.address = format!("{}/{}/{}.socket", self.work_dir, SIMULATED_SCAN_DIR, self.me.public_key);

            //check bcast_groups dir is there
            dir.push("bcast_groups"); //Dir is work_dir/bcast_groups
            if !dir.exists() {
                //Create bcast_groups dir
                try!(std::fs::create_dir(dir.as_path()));
                //info!("Created dir file {} ", dir.as_path().display());
            }

            //check current group dir is there
            let mut main_address = String::new();
            let groups = self.radios[0].broadcast_groups.clone();
            for group in groups.iter() {
                dir.push(&group); //Dir is work_dir/bcast_groups/&group
                
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
                        dir.pop(); //Dir is work_dir/bcast_groups
                        continue;
                    }
                    let _ = try!(std::os::unix::fs::symlink(&main_address, &linked_address));
                    //debug!("Pipe file {} created.", &linked_address);
                }
                
                dir.pop(); //Dir is work_dir/bcast_groups

            }
            //Now, remove the main address socket file. This will create broken symlinks in the broadcast
            //groups that this worker belongs to beyond the first one, but it's needed for the UnixListener
            //type that the file doesn't exist before starting to listen on it.
            //If the operation fails, we should error-out since the listener won't be able to start.
            let _ = fs::remove_file(main_address)?;

        } else {
            //Set TCP address
            self.me.address = format!("0.0.0.0:{}", DNS_SERVICE_PORT);
        }


        Ok(())
    }

    fn start_listener(&mut self) -> Result<(), WorkerError> {
        match self.operation_mode {
            OperationMode::Simulated => self.start_listener_simulated(),
            OperationMode::Device => self.start_listener_device(),
        }
    }

    fn start_listener_simulated(&mut self) -> Result<(), WorkerError> {
        let listener = try!(UnixListener::bind(&self.me.address));

        //Now listen for messages
        info!("Listening for messages.");
        for stream in listener.incoming() {
            match stream {
                Ok(client) => { 
                    let _result = try!(self.handle_client_simulated(client));
                },
                Err(e) => { 
                    warn!("Failed to connect to incoming client. Error: {}", e);
                },
            }
        }
        Ok(())
    }

    fn start_listener_device(&mut self) -> Result<(), WorkerError> {
        let listener = try!(TcpListener::bind(&self.me.address));
        debug!("Listening in address: {}", &self.me.address);

        //Now advertise the service to be discoverable by peers.
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
                    let _result = try!(self.handle_client_device(client));
                },
                Err(e) => { 
                    warn!("Failed to connect to incoming client. Error: {}", e);
                },
            }
        }
        Ok(())
    }
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
                     random_seed : 0, //The random seed itself doesn't need to be random. Also, that makes testing more difficult.
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
        obj.me.address_type = self.operation_mode.clone();
        obj.work_dir = self.work_dir;
        obj.random_seed = self.random_seed;
        obj.operation_mode = self.operation_mode;
        if obj.operation_mode == OperationMode::Device {
            obj.me.address = format!("0.0.0.0:{}", DNS_SERVICE_PORT);
        } else {
            obj.me.address = format!("{}/{}/{}.socket", obj.work_dir, SIMULATED_SCAN_DIR, obj.me.public_key);
        }
        obj.radios[0].broadcast_groups = self.broadcast_groups.unwrap_or(vec![]);
        obj.radios[0].reliability = self.reliability.unwrap_or(obj.radios[0].reliability);
        obj.radios[0].delay = self.delay.unwrap_or(obj.radios[0].delay);
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

        //file.flush().expect("Error flusing toml file to disk.");
        let file_name = format!("{}", file_path.display());
        Ok(file_name)
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
        let w_display = "Worker { radios: [Radio { delay: 0, reliability: 1, broadcast_groups: [] }], nearby_peers: {}, me: Peer { public_key: \"0000000000000000000000000000000000000000000000000000000000000000\", name: \"\", address: \"\", address_type: Simulated }, work_dir: \"\", random_seed: 0, operation_mode: Simulated, scan_interval: 1000, global_peer_list: Mutex { data: {} }, suspected_list: Mutex { data: [] } }";
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
        let radio_string = "Radio { delay: 0, reliability: 1, broadcast_groups: [] }";

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

    //**** WorkerConfig unit tests ****
    //Unit test for: WorkerConfig_new
    #[test]
    fn test_workerconfig_new() {
        let obj = WorkerConfig::new();

        assert_eq!(obj.broadcast_groups, Some(vec!("group1".to_string())));
        assert_eq!(obj.delay, Some(0));
        assert_eq!(obj.operation_mode, OperationMode::Simulated);
        assert_eq!(obj.random_seed, 0);
        assert_eq!(obj.reliability, Some(1.0));
        assert_eq!(obj.scan_interval, Some(2000));
        assert_eq!(obj.work_dir, ".".to_string());
        assert_eq!(obj.worker_name, "worker1".to_string());
    }

    //Unit test for: WorkerConfig::create_worker
    #[test]
    fn test_workerconfig_create_worker() {
        //test_workerconfig_new and test_worker_new already test the correct instantiation of WorkerConfig and Worker.
        //Just make sure thing function translates a WorkerConfig correctly into a Worker.
        let conf = WorkerConfig::new();
        let worker = conf.create_worker();
        let default_worker_display = "Worker { radios: [Radio { delay: 0, reliability: 1, broadcast_groups: [\"group1\"] }], nearby_peers: {}, me: Peer { public_key: \"0000000000000000000000000000000000000000000000000000000000000000\", name: \"worker1\", address: \"./bcast_groups/0000000000000000000000000000000000000000000000000000000000000000.socket\", address_type: Simulated }, work_dir: \".\", random_seed: 0, operation_mode: Simulated, scan_interval: 2000, global_peer_list: Mutex { data: {} }, suspected_list: Mutex { data: [] } }";
        assert_eq!(format!("{:?}", worker), String::from(default_worker_display));
    }

    //Unit test for: WorkerConfig::create_worker
    #[test]
    fn test_workerconfig_write_to_file() {
        let config = WorkerConfig::new();
        let mut path = std::env::temp_dir();
        path.push("worker.toml");

        let val = config.write_to_file(&path).expect("Could not write configuration file.");
        assert_eq!(val, format!("{}", path.display()));
        assert!(path.exists());

    }

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