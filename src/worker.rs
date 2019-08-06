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
#![deny(
    missing_docs,
    trivial_numeric_casts,
    unstable_features,
    unused_import_braces
)]

use crate::worker::listener::Listener;
use crate::worker::protocols::*;
use crate::worker::radio::*;
use crate::{MeshSimError, MeshSimErrorKind};
use byteorder::{NativeEndian, WriteBytesExt};
use libc::{c_int, nice};
use md5::Digest;
use rand::{rngs::StdRng, SeedableRng};
use serde_cbor::de::*;
use serde_cbor::ser::*;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::process::Child;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::sync::{MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::{self, error, fmt, io};

//Sub-modules declaration
pub mod commands;
pub mod listener;
pub mod mobility;
pub mod protocols;
pub mod radio;
pub mod worker_config;

// *****************************
// ********** Globals **********
// *****************************
//const DNS_SERVICE_NAME : &'static str = "meshsim";
//const DNS_SERVICE_TYPE : &'static str = "_http._tcp";
const DNS_SERVICE_PORT: u16 = 23456;
const WORKER_POOL_SIZE: usize = 2;
const SYSTEM_THREAD_NICE: c_int = -20; //Threads that need to run with a higher priority will use this

// *****************************
// ********** Structs **********
// *****************************

/// Type simplifying the signatures of functions returning a Radio and a Listener.
pub type Channel = (Arc<dyn Radio>, Box<dyn Listener>);

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

//region Errors
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

    fn cause(&self) -> Option<&dyn error::Error> {
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
    fn from(err: serde_cbor::Error) -> WorkerError {
        WorkerError::Serialization(err)
    }
}

impl From<io::Error> for WorkerError {
    fn from(err: io::Error) -> WorkerError {
        WorkerError::IO(err)
    }
}

impl<'a> From<PoisonError<MutexGuard<'a, HashSet<Peer>>>> for WorkerError {
    fn from(err: PoisonError<MutexGuard<'a, HashSet<Peer>>>) -> WorkerError {
        WorkerError::Sync(err.to_string())
    }
}

impl<'a> From<PoisonError<MutexGuard<'a, HashMap<String, Peer>>>> for WorkerError {
    fn from(err: PoisonError<MutexGuard<'a, HashMap<String, Peer>>>) -> WorkerError {
        WorkerError::Sync(err.to_string())
    }
}

impl<'a> From<PoisonError<MutexGuard<'a, HashMap<String, bool>>>> for WorkerError {
    fn from(err: PoisonError<MutexGuard<'a, HashMap<String, bool>>>) -> WorkerError {
        WorkerError::Sync(err.to_string())
    }
}

impl<'a> From<PoisonError<MutexGuard<'a, HashMap<String, String>>>> for WorkerError {
    fn from(err: PoisonError<MutexGuard<'a, HashMap<String, String>>>) -> WorkerError {
        WorkerError::Sync(err.to_string())
    }
}

impl<'a> From<PoisonError<MutexGuard<'a, HashMap<String, Vec<Vec<u8>>>>>> for WorkerError {
    fn from(err: PoisonError<MutexGuard<'a, HashMap<String, Vec<Vec<u8>>>>>) -> WorkerError {
        WorkerError::Sync(err.to_string())
    }
}

impl<'a>
    From<
        PoisonError<
            MutexGuard<'a, HashMap<String, protocols::reactive_gossip_routing::DataCacheEntry>>,
        >,
    > for WorkerError
{
    fn from(
        err: PoisonError<
            MutexGuard<'a, HashMap<String, protocols::reactive_gossip_routing::DataCacheEntry>>,
        >,
    ) -> WorkerError {
        WorkerError::Sync(err.to_string())
    }
}

impl<'a>
    From<
        PoisonError<
            MutexGuard<'a, HashMap<String, protocols::reactive_gossip_routing_II::DataCacheEntry>>,
        >,
    > for WorkerError
{
    fn from(
        err: PoisonError<
            MutexGuard<'a, HashMap<String, protocols::reactive_gossip_routing_II::DataCacheEntry>>,
        >,
    ) -> WorkerError {
        WorkerError::Sync(err.to_string())
    }
}

impl<'a> From<PoisonError<MutexGuard<'a, HashSet<String>>>> for WorkerError {
    fn from(err: PoisonError<MutexGuard<'a, HashSet<String>>>) -> WorkerError {
        WorkerError::Sync(err.to_string())
    }
}

impl From<rusqlite::Error> for WorkerError {
    fn from(err: rusqlite::Error) -> WorkerError {
        WorkerError::DB(err)
    }
}
//endregion Errors

/// Peer struct.
/// Defines the public identity of a node in the mesh.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, Default)]
pub struct Peer {
    /// Public key of the peer. It's considered it's public address.
    pub id: String,
    /// Friendly name of the peer.
    pub name: String,
    ///Endpoint at which this worker's short_radio is listening for messages.
    pub short_address: Option<String>,
    ///Endpoint at which this worker's long_radio is listening for messages.
    pub long_address: Option<String>,
    // ///The addesses that this peer is listening at.
    // addresses : Vec<AddressType>,
}

impl Peer {
    ///Empty constructor for the Peer struct.
    pub fn new() -> Peer {
        Peer {
            id: String::from(""),
            name: String::from(""),
            short_address: None,
            long_address: None,
        }
    }
}

/// Generic struct used to interface between the Worker and the protocols.
/// The sender and destination fields are used in the same way across all message-types and protocols.
/// The payload field encodes the specific data for the particular message type that only the protocol
/// knows about
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct MessageHeader {
    ///Sender of the message
    pub sender: Peer,
    ///Destination of the message
    pub destination: Peer,
    ///Number of hops this message has taken
    pub hops: u16,
    ///Optional, serialized payload of the message.
    /// It's the responsibility of the underlying protocol to know how to deserialize this payload
    /// into a protocol-specific message.
    pub payload: Option<Vec<u8>>,
}

impl MessageHeader {
    ///Creates a MessageHeader from a serialized vector of bytes.
    pub fn from_vec(data: Vec<u8>) -> Result<MessageHeader, serde_cbor::Error> {
        let msg: Result<MessageHeader, _> = from_reader(&data[..]);
        msg
    }

    ///Create new, empty MessageHeader.
    pub fn new() -> MessageHeader {
        MessageHeader {
            sender: Peer::new(),
            destination: Peer::new(),
            hops: 0u16,
            payload: None,
        }
    }

    /// Produces the MD5 checksum of this message based on the following data:
    /// Sender name
    /// Destination name
    /// Payload
    /// This is done instead of getting the md5sum of the entire structure for testability purposes
    pub fn get_hdr_hash(&self) -> Digest {
        let mut data = Vec::new();
        data.append(&mut self.destination.name.clone().into_bytes());
        if let Some(mut p) = self.payload.clone() {
            data.append(&mut p);
        }

        md5::compute(&data)
    }
}

/////Struct to represent DNS TXT records for advertising the meshsim service and obtain records from peers.
//#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
//pub struct ServiceRecord {
//    ///The service name.
//    service_name : String,
//    ///The service type.
//    service_type : String,
//    ///Name of the host advertising the service.
//    host_name : String,
//    ///Address of the host.
//    address : String,
//    ///What kind of address? (IPv4/IPv6)
//    address_type : String,
//    ///Port in which the service is listening.
//    port : u16,
//    ///Associated TXT records with this record.
//    txt_records : Vec<String>,
//}
//
//impl ServiceRecord {
//    ///Creates a new empty record
//    pub fn new() -> ServiceRecord {
//        ServiceRecord{  service_name : String::from(""),
//                        service_type: String::from(""),
//                        host_name : String::from(""),
//                        address : String::from(""),
//                        address_type : String::from(""),
//                        port : 0,
//                        txt_records : Vec::new() }
//    }
//
//    ///Return the TXT record value that matches the provided key. The key and values are separated by the '=' symbol on
//    ///service registration.
//    pub fn get_txt_record<'a>(&self, key : &'a str) -> Option<String> {
//        for t in &self.txt_records {
//            //Check record is in KEY=VALUE form.
//            let tokens = t.split('=').collect::<Vec<&str>>();
//            if tokens.len() == 2 && tokens[0] == key {
//                //Found the request record
//                return Some(String::from(tokens[1]))
//            }
//        }
//
//        None
//    }
//
//    ///Publishes the service using the mDNS protocol so other devices can discover it.
//    pub fn publish_service(service : ServiceRecord) -> Result<Child, WorkerError> {
//        //Constructing the external process call
//        let mut command = Command::new("avahi-publish");
//        command.arg("-s");
//        command.arg(service.service_name);
//        command.arg(service.service_type);
//        command.arg(service.port.to_string());
//
//        for r in service.txt_records {
//            command.arg(r.to_string());
//        }
//
//        // debug!("Registering service with command: {:?}", command);
//
//        //Starting the worker process
//        let child = command.spawn()?;
//        // info!("Process {} started.", child.id());
//
//        Ok(child)
//    }
//}

/// Operation modes for the worker.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
pub enum OperationMode {
    /// Simulated: The worker process is part of a simulated environment, running as one of many processes in the same machine.
    Simulated,
    /// Device: The worker is running on the actual hardware and has access to the radio transmitters.
    Device,
}

impl Default for OperationMode {
    fn default() -> Self {
        OperationMode::Simulated
    }
}

impl fmt::Display for OperationMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
        // or, alternatively:
        // fmt::Debug::fmt(self, f)
    }
}

impl FromStr for OperationMode {
    type Err = MeshSimError;

    fn from_str(s: &str) -> Result<OperationMode, MeshSimError> {
        let u = s.to_uppercase();
        match u.as_str() {
            "SIMULATED" => Ok(OperationMode::Simulated),
            "DEVICE" => Ok(OperationMode::Device),
            _ => {
                let err_msg = String::from("Unsupported operation mode.");
                let err = MeshSimError {
                    kind: MeshSimErrorKind::Configuration(err_msg),
                    cause: None,
                };
                Err(err)
            }
        }
    }
}

/// Worker struct.
/// Main struct for the worker module. Must be created via the ::new(Vec<Param>) method.
//#[derive(Debug, Clone)]
#[derive(Debug)]
pub struct Worker {
    ///Name of the current worker.
    name: String,
    ///Unique ID composed of 16 random numbers represented in a Hex String.
    pub id: String,
    /// Short-range radio for the worker.
    short_radio: Option<Channel>,
    /// Long-range radio for the worker.
    long_radio: Option<Channel>,
    ///Directory for the worker to operate. Must have RW access to it. Operational files and
    ///log files will be written here.
    work_dir: String,
    ///Random number generator used for all RNG operations.
    rng: Arc<Mutex<StdRng>>,
    ///Random number seed used for the rng of all processes.
    seed: u32,
    ///Simulated or Device operation.
    operation_mode: OperationMode,
    /// The protocol that this Worker should run for this configuration.
    pub protocol: Protocols,
    /// Logger for this Worker to use.
    logger: Logger,
}

impl Worker {
    /// The main function of the worker. The functions performs the following 3 functions:
    /// 1. Starts up all radios.
    /// 2. Joins the network.
    /// 3. It starts to listen for messages of the network on all addresss
    ///    defined by it's radio array. Upon reception of a message, it will react accordingly to the protocol.
    pub fn start(&mut self, accept_commands: bool) -> Result<(), MeshSimError> {
        //Init the worker
        self.init()?;

        //Init the radios and get their respective listeners.
        let short_radio = self.short_radio.take();
        let long_radio = self.long_radio.take();
        let mut resources = build_protocol_resources(
            self.protocol,
            short_radio,
            long_radio,
            self.seed,
            self.id.clone(),
            self.name.clone(),
            self.logger.clone(),
        )?;

        info!(self.logger, "Worker finished initializing.");

        //Initialize protocol.
        let _res = resources.handler.init_protocol()?;

        //Start listening for messages
        let prot_handler = Arc::clone(&resources.handler);
        let mut threads: Vec<JoinHandle<()>> = resources
            .radio_channels
            .drain(..)
            .map(|(rx, tx)| {
                let prot_handler = Arc::clone(&prot_handler);
                let logger = self.logger.clone();
                let thread_pool = threadpool::Builder::new()
                    .num_threads(WORKER_POOL_SIZE)
                    .build();

                thread::spawn(move || {
                    let radio_label: String = match rx.get_radio_range() {
                        RadioTypes::LongRange => String::from("LoraRadio"),
                        RadioTypes::ShortRange => String::from("WifiRadio"),
                    };
                    info!(logger, "[{}] Listening for messages", &radio_label);
                    loop {
                        match rx.read_message() {
                            Some(hdr) => {
                                let prot = Arc::clone(&prot_handler);
                                let r_type = rx.get_radio_range();
                                let tx_channel = Arc::clone(&tx);
                                let log = logger.clone();

                                thread_pool.execute(move || {
                                    let response = match prot.handle_message(hdr, r_type) {
                                        Ok(resp) => resp,
                                        Err(e) => {
                                            error!(log, "Error handling message:");
                                            error!(log, "{}", &e);
                                            if let Some(cause) = e.cause {
                                                error!(log, "Cause: {}", cause);
                                            }
                                            None
                                        }
                                    };

                                    if let Some(r) = response {
                                        match tx_channel.broadcast(r) {
                                            Ok(()) => { /* All good */ }
                                            Err(e) => {
                                                error!(log, "Error sending response: {}", e);
                                            }
                                        }
                                    }
                                });
                                debug!(logger, "Jobs in queue: {}", thread_pool.queued_count());
                                debug!(
                                    logger,
                                    "Jobs that have paniced: {}",
                                    thread_pool.panic_count()
                                );
                            }
                            None => {
                                warn!(logger, "Failed to read incoming message.");
                            }
                        }
                    }
                })
            })
            .collect();

        if accept_commands {
            let com_loop_thread = self
                .start_command_loop_thread(Arc::clone(&prot_handler))
                .map_err(|e| {
                    let err_msg = String::from("Failed to start command_loop_thread");
                    MeshSimError {
                        kind: MeshSimErrorKind::Worker(err_msg),
                        cause: Some(Box::new(e)),
                    }
                })?;
            threads.push(com_loop_thread);
        }

        // //DEBUG LINE
        // let alive_thread = self.start_second_counter_thread()?;
        // threads.push(alive_thread);

        // let _exit_values = threads.map(|x| x.map(|t| t.join())); //This compact version does not work. It seems the lazy iterator is not evaluated.
        for x in threads {
            // if let Some(h) = x {
            //     debug!("Waiting for a listener thread...");
            let _res = x.join();
            // }
        }

        Ok(())
    }

    fn init(&mut self) -> Result<(), MeshSimError> {
        //        //Make sure the required directories are there and writable or create them.
        //        //check log dir is there
        //        let mut dir = std::fs::canonicalize(&self.work_dir)?;
        //        dir.push("log"); //Dir is work_dir/log
        //        if !dir.exists() {
        //            //Create log dir
        //            std::fs::create_dir_all(dir.as_path()).unwrap_or(());
        //            info!(self.logger, "Created dir {} ", dir.as_path().display());
        //        }
        //        dir.pop(); //Dir is work_dir

        Ok(())
    }

    ///Returns a random number generator seeded with the passed parameter.
    pub fn rng_from_seed(seed: u32) -> StdRng {
        //Create RNG from the provided random seed.
        let mut random_bytes = vec![];
        //Fill the uper 28 bytes with 0s
        for _i in 0..7 {
            random_bytes.write_u32::<NativeEndian>(0).unwrap_or(());
        }
        //Write the last 4 bytes from the provided seed
        random_bytes.write_u32::<NativeEndian>(seed).unwrap_or(());

        // let randomness : Vec<usize> = random_bytes.iter().map(|v| *v as usize).collect();
        let mut randomness = [0; 32];
        randomness.copy_from_slice(random_bytes.as_slice());
        StdRng::from_seed(randomness)
    }

    fn start_command_loop_thread(
        &self,
        protocol_handler: Arc<dyn Protocol>,
    ) -> io::Result<JoinHandle<()>> {
        let tb = thread::Builder::new();
        let logger = self.logger.clone();

        tb.name(String::from("CommandLoop")).spawn(move || {
            unsafe {
                let new_nice = nice(SYSTEM_THREAD_NICE);
                debug!(logger, "[CommandLoop]: New priority: {}", new_nice);
            }
            Worker::command_loop(&logger, protocol_handler);
        })
    }

    // Debug function, only used to make sure the worker is alive and getting scheduled.
    //    fn start_second_counter_thread(&self) -> io::Result<JoinHandle<Result<(), MeshSimError>>> {
    //        let tb = thread::Builder::new();
    //        let logger = self.logger.clone();
    //        let mut i = 0;
    //
    //        tb.name(String::from("AliveThread"))
    //        .spawn(move || {
    //            loop{
    //                thread::sleep(Duration::from_millis(1000));
    //                info!(logger, "{}", i);
    //                i += 1;
    //            }
    //            Ok(())
    //        })
    //    }

    fn command_loop(logger: &Logger, protocol_handler: Arc<dyn Protocol>) {
        let mut input = String::new();
        let stdin = io::stdin();

        info!(logger, "Command loop started");
        loop {
            match stdin.read_line(&mut input) {
                Ok(_bytes) => {
                    match input.parse::<commands::Commands>() {
                        Ok(command) => {
                            match Worker::process_command(
                                command,
                                Arc::clone(&protocol_handler),
                                logger,
                            ) {
                                Ok(_) => { /* All good! */ }
                                Err(e) => {
                                    error!(logger, "Error executing command: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!(logger, "Error parsing command: {}", e);
                        }
                    }
                }
                Err(error) => {
                    error!(logger, "{}", error);
                }
            }
            input.clear();
        }
        //        #[allow(unreachable_code)]
        //        Ok(()) //Loop should never end
    }

    fn process_command(
        com: commands::Commands,
        ph: Arc<dyn Protocol>,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        match com {
            commands::Commands::Send(destination, data) => {
                info!(logger, "Send command received");
                ph.send(destination, data)?;
            }
        }
        Ok(())
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
    use std::iter;

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

    // //**** ServiceRecord unit tests ****
    // //Unit test for get get_txt_record
    // #[test]
    // fn test_get_txt_record() {
    //     let mut record = ServiceRecord::new();
    //     record.txt_records.push(String::from("NAME=Worker1"));

    //     assert_eq!(String::from("Worker1"), record.get_txt_record("NAME").unwrap());
    // }

    #[test]
    fn test_message_header_hash() {
        let mut msg = MessageHeader::new();
        msg.sender.name = String::from("SENDER");
        msg.destination.name = String::from("DESTINATION");
        let mut rng = Worker::rng_from_seed(12345);
        let mut data: Vec<u8> = iter::repeat(0u8).take(64).collect();

        rng.fill_bytes(&mut data[..]);
        msg.payload = Some(data.clone());
        let hash = msg.get_hdr_hash();
        assert_eq!(&format!("{:x}", &hash), "16c37d4dbeed437d377fc131b016cf38");

        rng.fill_bytes(&mut data[..]);
        msg.payload = Some(data.clone());
        let hash = msg.get_hdr_hash();
        assert_eq!(&format!("{:x}", &hash), "48b9f1d803d6829bcab59056981e6771");

        rng.fill_bytes(&mut data[..]);
        msg.payload = Some(data.clone());
        let hash = msg.get_hdr_hash();
        assert_eq!(&format!("{:x}", &hash), "a6048051b8727b181a6c69edf820914f");
    }

    //This test will me removed at some point, but it's useful at the moment for the message size calculations
    //that will be needed to do segmentation of large pieces of data for transmission.
    #[ignore]
    #[test]
    fn test_message_size() {
        use std::mem;

        let mut self_peer = Peer::new();
        self_peer.name = String::from("Foo");
        self_peer.id = String::from("kajsndlkajsndaskdnlkjsadnks");

        let mut dest_peer = Peer::new();
        dest_peer.name = String::from("Bar");
        dest_peer.id = String::from("oija450njjcdlhbaslijdblahsd");

        let hdr = MessageHeader::new();
        //        hdr.sender = self_peer.clone();
        //        hdr.destination = dest_peer.clone();

        println!("Size of MessageHeader: {}", mem::size_of::<MessageHeader>());
        let serialized_hdr = to_vec(&hdr).expect("Could not serialize");
        let hdr_size = mem::size_of_val(&serialized_hdr);
        println!("Size of a serialized MessageHeader: {}", hdr_size);

        println!("Size of Peer: {}", mem::size_of::<Peer>());
        let serialized_peer = to_vec(&self_peer).expect("Could not serialize");
        let peer_size = mem::size_of_val(&serialized_peer);
        println!("Size of a Peer instance: {}", peer_size);

        //        let mut msg = DataMessage{ route_id : String::from("SOME_ROUTE"),
        //            payload :  String::from("SOME DATA TO TRANSMIT").as_bytes().to_vec() };
        //        println!("Size of DataMessage: {}", mem::size_of::<DataMessage>());
        //        let msg_size = mem::size_of_val(&msg);
        //        println!("Size of a MessageHeader instance: {}", msg_size);
        //        let ser_msg = to_vec(&msg).expect("Could not serialize");
        //        println!("Size of ser_msg: {}", mem::size_of_val(&ser_msg));
        //        println!("Len of ser_msg: {}", ser_msg.len());
        //        let ser_hdr = to_vec(&hdr).expect("Could not serialize");
        //        println!("Len of ser_hdr: {}", ser_hdr.len());
        //        hdr.payload = Some(ser_msg);

        //        let final_hdr = to_vec(&hdr).expect("Could not serialize");
        //        println!("Len of final_hdr: {}", final_hdr.len());
        //
        //        println!("Size of SampleStruct: {}", mem::size_of::<SampleStruct>());
        //        let s = SampleStruct;
        //        let xs = to_vec(&s).expect("Could not serialize");
        //        println!("Size of serialized SampleStruct: {}", xs.len());
        //        println!("xs: {:?}", &xs);

        //        let sample_data = [1u8; 2048];
        //        println!("sample data len: {}", sample_data.len());
        //        println!("sample data sizeof: {}", mem::size_of_val(&sample_data));
        //        let ser_sample_data = to_vec(&sample_data.to_vec()).expect("Could not serialize");
        //        println!("ser_sample_data len: {}", ser_sample_data.len());
        assert!(false);
    }
}

// *****************************
// ******** End tests **********
// *****************************
