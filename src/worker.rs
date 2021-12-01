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
// missing_docs,
    trivial_numeric_casts,
    unstable_features,
    unused_import_braces
)]

use crate::*;
use crate::worker::listener::Listener;
use crate::worker::protocols::*;
use crate::worker::radio::*;
use crate::master::Master;
use crate::{MeshSimError, MeshSimErrorKind};
use crate::mobility::{Position, Velocity};
use crate::master::REG_SERVER_LISTEN_PORT;
use byteorder::{NativeEndian, WriteBytesExt};
// use libc::{c_int};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::io::ErrorKind;
use chrono::TimeZone;
use chrono::Utc;
use rand::{rngs::StdRng, SeedableRng};
// use serde_cbor::de::*;
// use serde_cbor::ser::*;
use slog::{Key, Logger, Record, Serializer, Value};
use std::{collections::{HashMap, HashSet}, path::PathBuf};
use std::io::Write;
use socket2::{Socket, Domain, Type, SockAddr};
use rand::{thread_rng, RngCore};

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::{self, error, fmt, io};
use std::time::Duration;

//Sub-modules declaration
pub mod commands;
pub mod listener;
// pub mod mobility;
pub mod protocols;
pub mod radio;
pub mod worker_config;

// *****************************
// ********** Globals **********
// *****************************
//const DNS_SERVICE_NAME : &'static str = "meshsim";
//const DNS_SERVICE_TYPE : &'static str = "_http._tcp";
const DNS_SERVICE_PORT: u16 = 23456;
const WORKER_POOL_SIZE: usize = 1;
const MAIN_THREAD_PERIOD: u64 = ONE_MILLISECOND_NS;
/// subdirectory name where the command sockets are placed
pub const SOCKET_DIR: &str = "sockets";
const COMMAND_PERIOD: u64 = ONE_MILLISECOND_NS;
// Period for logging statistics regarding the state of the worker
const STATS_PERIOD: i64 = ONE_SECOND_NS as i64;
const REGISTRATION_RETRIES: usize = 5;

// *****************************
// ********** Structs **********
// *****************************

/// Type simplifying the signatures of functions returning a Radio and a Listener.
pub type Channel = (Arc<dyn Radio>, Box<dyn Listener>);

/// Error type for all possible errors generated in the Worker module.
#[derive(Debug)]
pub enum WorkerError {
    ///Error while serializing data with Serde.
    Serialization(bincode::Error),
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
    // fn description(&self) -> &str {
    //     match *self {
    //         WorkerError::Serialization(ref err) => err.description(),
    //         WorkerError::IO(ref err) => err.description(),
    //         WorkerError::Configuration(ref err) => err.as_str(),
    //         WorkerError::Sync(ref err) => err.as_str(),
    //         WorkerError::Command(ref err) => err.as_str(),
    //         WorkerError::DB(ref err) => err.description(),
    //     }
    // }

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

impl From<bincode::Error> for WorkerError {
    fn from(err: bincode::Error) -> WorkerError {
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
// #[table_name="workers"]
pub struct Peer {
    /// Friendly name of the peer.
    pub name: String,
    /// Public key of the peer. It's considered it's public address.
    pub id: String,
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
    pub sender: String,
    ///Destination of the message
    pub destination: String,
    ///Number of hops this message has taken
    pub hops: u16,
    ///Indication for the simulated radio of how long to delay the reception of a message for
    pub delay: i64,
    /// Number of hops until the message is discarded
    pub ttl: usize,
    // Signal strength used to send this message in decibels.
    pub signal_strength: f64,
    ///Optional, serialized payload of the message.
    /// It's the responsibility of the underlying protocol to know how to deserialize this payload
    /// into a protocol-specific message.
    payload: Vec<u8>,
    ///A hash value that (semi)uniquely identifies this message
    msg_id: String,
}

impl MessageHeader {
    ///Creates a MessageHeader from a serialized vector of bytes.
    pub fn from_vec(data: Vec<u8>) -> Result<MessageHeader, MeshSimError> {
        let msg = bincode::deserialize(&data[..])
            .map_err(|e| {
                let err_msg = String::from("Failed to deserialize message");
                MeshSimError {
                    kind: MeshSimErrorKind::Serialization(err_msg),
                    cause: Some(Box::new(e)),
                }
        })?;
        Ok(msg)
    }

    pub fn to_vec(&self) -> Result<Vec<u8>, MeshSimError> {
        let encoded = bincode::serialize(&self)
            .map_err(|e| {
                let err_msg = String::from("Failed to serialize message");
                MeshSimError {
                    kind: MeshSimErrorKind::Serialization(err_msg),
                    cause: Some(Box::new(e)),
                }
        })?;
        Ok(encoded)
    }

    ///Create new, empty MessageHeader.
    pub fn new(sender: String, destination: String, payload: Vec<u8>) -> MessageHeader {
        let msg_id = MessageHeader::create_msg_id(payload.clone());
        MessageHeader {
            sender,
            destination,
            hops: 0u16,
            delay: Utc::now().timestamp_nanos(),
            ttl: std::usize::MAX,
            signal_strength: 0f64,
            payload,
            msg_id,
        }
    }

    ///Used to create another header based on the current one. This is the recommended way
    ///to create response messages in order to preserve metadata such as TTL and hops, which
    ///are updated automatically.
    pub fn create_forward_header(self, new_sender: String) -> MessageHeaderBuilder {
        MessageHeaderBuilder {
            old_data: self,
            sender: new_sender,
            destination: None,
            hops: None,
            ttl: None,
            signal_strength: None,
            payload: None,
        }
    }

    /// Produces the MD5 checksum of this message based on the following data:
    /// Sender name
    /// Destination name
    /// Payload
    /// This is done instead of getting the md5sum of the entire structure for testability purposes
    pub fn get_msg_id(&self) -> &str {
        &self.msg_id
    }

    /// Get a reference to the payload of the message. It is primarily used to deserialize it into a protocol message.
    pub fn get_payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    fn create_msg_id(mut payload: Vec<u8>) -> String {
        let mut data = Vec::new();
        // data.append(&mut destination.to_string().into_bytes());
        let mut ts = Utc::now().timestamp_nanos().to_le_bytes().to_vec();
        data.append(&mut ts);
        data.append(&mut payload);

        let d = md5::compute(&data);
        format!("{:x}", d)
    }
}

///Used to create a new MessageHeader from another MessageHeader.
///Created with the method create_forward_header from MessageHeader.
pub struct MessageHeaderBuilder {
    old_data: MessageHeader,
    /// The new sender of the message
    sender: String,
    ///Destination of the message
    destination: Option<String>,
    ///Number of hops this message has taken
    hops: Option<u16>,
    /// Number of hops until the message is discarded
    ttl: Option<usize>,
    /// Signal strength used to send the message in decibels.
    signal_strength: Option<f64>,
    /// It's the responsibility of the underlying protocol to know how to deserialize this payload
    /// into a protocol-specific message.
    payload: Option<Vec<u8>>,
}

impl MessageHeaderBuilder {
    ///Sets a new destination for the MessageHeader. By default, the previous destination is retained.
    pub fn set_destination(mut self, dest: String) -> MessageHeaderBuilder {
        self.destination = Some(dest);
        self
    }

    ///Sets a new hops count for the MessageHeader. By default, the previous count is retained.
    pub fn set_hops(mut self, hops: u16) -> MessageHeaderBuilder {
        self.hops = Some(hops);
        self
    }

    ///Sets a new TTL for the MessageHeader. By default, the previous count is retained.
    pub fn set_ttl(mut self, ttl: usize) -> MessageHeaderBuilder {
        self.ttl = Some(ttl);
        self
    }

    ///Sets a new payload for the MessageHeader. Should be called if the payload has changed at all.
    ///By default, the previous payload is retained.
    pub fn set_payload(mut self, payload: Vec<u8>) -> MessageHeaderBuilder {
        self.payload = Some(payload);
        self
    }

    ///Consumes the current MessageHeaderBuilder to produce a MessageHeader
    pub fn build(self) -> MessageHeader {
        let destination = self
            .destination
            .unwrap_or(self.old_data.destination.clone());
        let msg_id = self.old_data.get_msg_id().to_string();
        let payload = self.payload.unwrap_or(self.old_data.payload);
        // let msg_id = MessageHeader::create_msg_id(payload.clone());

        MessageHeader {
            sender: self.sender,
            destination,
            hops: self.hops.unwrap_or(self.old_data.hops),
            delay: Utc::now().timestamp_nanos(),
            ttl: self.ttl.unwrap_or(self.old_data.ttl),
            signal_strength: self.signal_strength.unwrap_or(self.old_data.signal_strength),
            payload,
            msg_id,
        }
    }
}
/// Enum that represents the possible status of a Message as it moves through the network
pub enum MessageStatus {
    /// The message has reached its destination.
    ACCEPTED,
    /// The message has been dropped. The *reason* field should provide more data.
    DROPPED,
    /// The message has reached an intermediate node and will be forwarded.
    FORWARDING,
    /// A new message has been transmitted
    SENT,
    /// The message has been queued. This usually indicates a pending route operation.
    QUEUED,
}

impl fmt::Display for MessageStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MessageStatus::ACCEPTED => write!(f, "ACCEPTED"),
            MessageStatus::DROPPED => write!(f, "DROPPED"),
            MessageStatus::FORWARDING => write!(f, "FORWARDING"),
            MessageStatus::SENT => write!(f, "SENT"),
            MessageStatus::QUEUED => write!(f, "QUEUED"),
        }
    }
}

impl Value for MessageStatus {
    fn serialize(&self, _rec: &Record, key: Key, serializer: &mut dyn Serializer) -> slog::Result {
        serializer.emit_str(key, &self.to_string())
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
    ///UDS through which the worker receives commands
    cmd_socket: Socket,
    ///Listen address for the cmd sock.
    cmd_socket_addr: String,
    /// The protocol that this Worker should run for this configuration.
    pub protocol: Protocols,
    /// The maximum number of queued packets a worker can have
    packet_queue_size: usize,
    ///Threshold after which a packet is considered stale and dropped.
    ///Expressed in nanoseconds.
    stale_packet_threshold: i64,
    /// Logger for this Worker to use.
    logger: Logger,
}

impl Worker {
    fn new(
        name: String,
        sr_channels: Option<(Arc<dyn Radio>, Box<dyn Listener>)>,
        lr_channels: Option<(Arc<dyn Radio>, Box<dyn Listener>)>,
        work_dir: String,
        rng: Arc<Mutex<StdRng>>,
        seed: u32,
        operation_mode: OperationMode,
        id: String,
        protocol: Protocols,
        packet_queue_size: usize,
        stale_packet_threshold: i64,
        pos : Position,
        vel: Option<Velocity>,
        dest: Option<Position>,
        logger: Logger,

    ) -> Result<Self, MeshSimError> {

        // Initialise the command socket
        let (socket, sock_address) = Worker::new_command_socket(&work_dir, &name)?;

        let mut w = Worker {
            name,
            short_radio: sr_channels,
            long_radio: lr_channels,
            work_dir,
            rng,
            seed,
            cmd_socket: socket,
            cmd_socket_addr: sock_address,
            id,
            protocol,
            packet_queue_size,
            stale_packet_threshold,
            logger,
        };
        let _res = w.init(operation_mode, pos, vel, dest)?;

        Ok(w)
    }

    fn new_command_socket(work_dir: &String, name: &String) -> Result<(Socket, String), MeshSimError> {
        let mut command_address = PathBuf::from(work_dir);
        command_address.push(SOCKET_DIR);
        //Make sure the directory exits
        if !command_address.exists() {
            std::fs::create_dir(&command_address).map_err(|e| {
                let err_msg = String::from("Failed to create socket directory");
                MeshSimError {
                    kind: MeshSimErrorKind::Configuration(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;
        }
        command_address.push(format!("{}.sock", name));
        let socket = Socket::new(Domain::unix(), Type::dgram(), None)
        .map_err(|e| {
                let err_msg = String::from("Failed to create command server socket");
                MeshSimError {
                    kind: MeshSimErrorKind::Configuration(err_msg),
                    cause: Some(Box::new(e)),
                }
        })?;
        let sock_addr = SockAddr::unix(&command_address)
        .map_err(|e| {
                let err_msg = String::from("Failed to create UDS address");
                MeshSimError {
                    kind: MeshSimErrorKind::Configuration(err_msg),
                    cause: Some(Box::new(e)),
                }
        })?;
        //Bind the server
        socket.bind(&sock_addr)
        .map_err(|e| {
            let err_msg = String::from("Failed to start command server");
            MeshSimError {
                kind: MeshSimErrorKind::Configuration(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

        //Finally, set the socket in non-blocking mode
        let read_time = std::time::Duration::from_nanos(COMMAND_PERIOD);
        socket
            .set_read_timeout(Some(read_time))
            // .set_nonblocking(true)
            .map_err(|e| {
                let err_msg = String::from("Coult not set socket on non-blocking mode");
                MeshSimError {
                    kind: MeshSimErrorKind::Configuration(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;

        Ok((socket, command_address.as_path().display().to_string()))
    }

    /// The main loop of the worker.
    pub fn start(&mut self, _accept_commands: bool) -> Result<(), MeshSimError> {
        let finish = Arc::new(AtomicBool::new(false));
        let mut buffer = [0; 65536];

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

        //Initialize protocol.
        let _res = resources.handler.init_protocol()?;
        //Start listening for messages
        let prot_handler = Arc::clone(&resources.handler);
        let threads: Vec<JoinHandle<String>> = resources
            .radio_channels
            .drain(..)
            .map(|(rx, (tx, _out_queue_sender, out_queue_receiver))| {
                let prot_handler = Arc::clone(&prot_handler);
                let logger = self.logger.clone();
                let in_queue_thread_pool = threadpool::Builder::new()
                    .num_threads(WORKER_POOL_SIZE)
                    .build();
                let max_queued_jobs = self.packet_queue_size;
                let stale_packet_threshold = self.stale_packet_threshold;
                let r_label: String = rx.get_radio_range().into();
                let finish = Arc::clone(&finish);
                let thread_name = format!("RadioListener[{}]", &r_label);

                thread::Builder::new()
                .name(thread_name.clone())
                .spawn(move || -> String {
                    info!(logger, "[{}] Listening for messages", &r_label);
                    let mut stats_ts = Utc::now();

                    while !finish.load(Ordering::SeqCst)  {
                        let radio_label = r_label.clone();
                        let rl = radio_label.clone();

                        /**************************************************
                        ***************  STATISTICS STAGE   ***************
                        **************************************************/
                        if stats_ts.timestamp_nanos() + STATS_PERIOD <= Utc::now().timestamp_nanos() {
                            info!(
                                logger,
                                "Worker stats";
                                "IN_QUEUE_LENGTH" => in_queue_thread_pool.queued_count(),
                                "OUT_QUEUE_LENGTH" => out_queue_receiver.len(),
                                "WORKERS_ACTIVE" => in_queue_thread_pool.active_count(),
                                "WORKERS_PANICKED" => in_queue_thread_pool.panic_count(),
                            );

                            stats_ts = Utc::now();
                        }

                        /**************************************************
                        ***************  RADIO  RX  STAGE   ***************
                        **************************************************/
                        // Read any new messages over the current radio
                        match rx.read_message() {
                            Some(mut hdr) => {
                                //Store the timestamp when this message was queued
                                hdr.delay = Utc::now().timestamp_nanos();
                                let prot = Arc::clone(&prot_handler);
                                let r_type = rx.get_radio_range();
                                let log = logger.clone();

                                if in_queue_thread_pool.queued_count() >= max_queued_jobs {
                                    let log_data = ();
                                    radio::log_handle_message(
                                        &log,
                                        &hdr,
                                        MessageStatus::DROPPED, 
                                        Some("packet_queue is full"),
                                        None,
                                        r_type,
                                        &log_data,
                                    );
                                    continue;
                                }

                                in_queue_thread_pool.execute(move || {
                                    let ts0 = Utc.timestamp_nanos(hdr.delay);
                                    let perf_in_queued_duration =
                                        Utc::now().timestamp_nanos() - ts0.timestamp_nanos();
                                    info!(
                                        &log,
                                        "in_queued";
                                        "msg_id" => hdr.get_msg_id(),
                                        "duration" => perf_in_queued_duration,
                                        "radio" => &rl,
                                    );

                                    if perf_in_queued_duration > stale_packet_threshold {
                                        warn!(
                                            log,
                                            "Skipping message";
                                            "destination" => &hdr.destination,
                                            "source" => &hdr.sender,
                                            "reason" => "stale packet",
                                            "status" => MessageStatus::DROPPED,
                                            "msg_id" => &hdr.msg_id,
                                            "radio" => &rl,
                                        );
                                        return;
                                    }
                                    
                                    //Perf_Recv_Msg_Start
                                    hdr.delay = Utc::now().timestamp_nanos();
                                    // let (response, log_data) =
                                    match prot.handle_message(hdr, ts0, r_type) {
                                        Ok(_) => {
                                            /**/
                                        },
                                        Err(e) => {
                                            error!(log, "Error handling message:");
                                            error!(log, "{}", &e);
                                            if let Some(cause) = e.cause {
                                                error!(log, "Cause: {}", cause);
                                            }
                                        }
                                    };
                                });

                            }
                            None => {
                                /* No new messages. Proceed to the next section */
                            }
                        }

                        /**************************************************
                        ***************  RADIO  TX  STAGE   ***************
                        **************************************************/
                        // Pop the top pending message from the out_queue (if any) and transmit it
                        let resp = out_queue_receiver.try_recv().ok();
                        if let Some((mut resp_hdr, log_data, out_queue_start)) = resp {
                            let tx_channel = Arc::clone(&tx);
                            let log = logger.clone();

                            //Is the packet stale?
                            if (Utc::now().timestamp_nanos() - out_queue_start.timestamp_nanos()) > 
                                stale_packet_threshold {
                                warn!(
                                    log,
                                    "Not transmitting";
                                    "destination" => &resp_hdr.destination,
                                    "source" => &resp_hdr.sender,
                                    "reason" => "stale packet",
                                    "status" => MessageStatus::DROPPED,
                                    "msg_id" => &resp_hdr.msg_id,
                                    "radio" => &radio_label,
                                );
                                continue;
                            }

                            //perf_out_queued_start
                            resp_hdr.delay = out_queue_start.timestamp_nanos();
                            
                            // let _msg_id = r.get_msg_id().to_string();
                            match tx_channel.broadcast(resp_hdr.clone()) {
                                Ok(tx) => {
                                    if let Some(tx) = tx {
                                        /* Log transmission */
                                        radio::log_tx(
                                            &log,
                                            tx,
                                            &resp_hdr.msg_id,
                                            MessageStatus::SENT,
                                            &resp_hdr.sender,
                                            &resp_hdr.destination,
                                            log_data,
                                        );
                                    }
                                }
                                Err(e) => {
                                    error!(log, "Error sending response: {}", e);
                                    if let Some(cause) = e.cause {
                                        error!(log, "Cause: {}", cause);
                                    }
                                }
                            }
                        }
                    }

                    thread_name
                }).expect("Could not spawn radio thread")
            })
            .collect();

        /*
        After spawning the listener threads for each radio, the main thread remain in this loop
        periodically executing the maintenance operations of the protocol handler.

        Also in the future, it would be great if instead of having a dedicated logger, the log macros pushed the
        log records into an mpsc channel that was the written by this thread when not doing maintenance tasks.

        With this design, the master could send a stop command to the worker instead of a SIGTERM signal, at which
        point the worker could kill the radio threads, stop doing maintenance work, and focus on flushing the log
        records before exiting.
        */
        let main_thread_period = Duration::from_nanos(MAIN_THREAD_PERIOD);
        while !finish.load(Ordering::SeqCst) {
            thread::sleep(main_thread_period);

            // Check if any maintenance work needs be done.
            match prot_handler.do_maintenance() {
                Ok(_) => { /* All good! */ }
                Err(mut e) => {
                    let cause = e
                        .cause
                        .take()
                        .map(|x| format!("{}", x));
                    error!(
                        self.logger,
                        "Failed to perform maintenance operations: {}", e;
                        "Cause" => cause,
                    );

                }
            }

            // Check if new commands have been received
            // Keep this section at the end of the main thread loop since it skips the rest of the iteration
            // when no data is available.
            let (bytes_read, _peer_addr) = match self.cmd_socket.recv_from(&mut buffer) {
                Ok((bytes, addr)) => { (bytes, addr)},
                Err(e) => {
                    if e.kind() != ErrorKind::WouldBlock {
                        error!(&self.logger, "Error reading from command socket: {}", &e);
                    }
                    continue;
                }
            };

            if bytes_read > 0 {
                let cmd: Option<commands::Commands> = bincode::deserialize(&buffer[..bytes_read]).ok();
                match Worker::process_command(
                    cmd,
                    Arc::clone(&prot_handler),
                    &finish,
                    &self.logger,
                ) {
                    Ok(_) => { /* All good! */ }
                    Err(e) => {
                        error!(&self.logger, "Error executing command: {}", &e);
                        if let Some(cause) = e.cause {
                            error!(&self.logger, "Cause: {}", &cause);
                        }
                    }
                }
            }

        }

        for h in threads {
            let name = match h.join() {
                Ok(s) => s,
                Err(e) => {
                    info!(&self.logger, "Thread finished with an error: {:?}", &e);
                    continue;
                }
            };
            info!(&self.logger, "{} thread finished", &name);
        }

        /*
        The finish command has been received. Give the process a bit of time to finish writing logs.
        This is due to the fact that currently the logging is done asynchronously by a separate thread, 
        and some times the logs are truncated in the middle of a message.
        */
        // thread::sleep(Duration::from_nanos(10 * ONE_MILLISECOND_NS));

        info!(&self.logger, "Worker exiting");

        Ok(())
    }

    fn init(
        &mut self,
        operation_mode: OperationMode,
        pos : Position,
        vel: Option<Velocity>,
        dest: Option<Position>,
    ) -> Result<(), MeshSimError> {

        match operation_mode  {
            OperationMode::Device => {
                /* Nothing to do */
                return Ok(())
            },
            OperationMode::Simulated => {
                let sr = self.short_radio.take();
                let sr_address = if let Some((radio, listener)) = sr {
                    let sr_address = listener.get_address();
                    self.short_radio = Some((radio, listener));
                    Some(sr_address)
                } else {
                    None
                };

                let lr = self.long_radio.take();
                let lr_address = if let Some((radio, listener)) = lr {
                    let lr_address = listener.get_address();
                    self.long_radio = Some((radio, listener));
                    Some(lr_address)
                } else {
                    None
                };

                //Register the worker with the master
                let cmd = commands::Commands::RegisterWorker{
                    w_name: self.name.clone(),
                    w_id: self.id.clone(),
                    pos,
                    vel,
                    dest,
                    sr_address,
                    lr_address,
                    cmd_address: self.cmd_socket_addr.clone(),
                };
                let data = bincode::serialize(&cmd)
                    .map_err(|e|{
                        let err_msg = String::from("Failed to serialise registration command");
                        MeshSimError {
                            kind: MeshSimErrorKind::Serialization(err_msg),
                            cause: Some(Box::new(e)),
                        }
                    })?;

                let sock = Worker::connect_to_registration_server(&self.logger)?;
                let local_addr = sock.local_addr()
                .map_err(|e| {
                        let err_msg = String::from("Failed to extract local address of registration socket");
                        MeshSimError {
                            kind: MeshSimErrorKind::Networking(err_msg),
                            cause: Some(Box::new(e)),
                        }
                })?;

                //Log the local address so that it can be matched to any potential errors on the server side.
                info!(self.logger, "Connected to registration server with local_address: {:?}", &local_addr);

                let bytes_written = sock.send(&data)
                    .map_err(|e|{
                        let err_msg = String::from("Failed to send registration command");
                        MeshSimError {
                            kind: MeshSimErrorKind::Networking(err_msg),
                            cause: Some(Box::new(e)),
                        }
                    })?;
                info!(self.logger, "Registration command sent"; "bytes_written" => bytes_written);
            },
        }

        info!(self.logger, "Worker finished initializing.");

        Ok(())
    }

    fn connect_to_registration_server(logger: &Logger) -> Result<Socket, MeshSimError> {
        let conn_timeout = Duration::from_nanos(10 * ONE_MILLISECOND_NS);
        let master_addr = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
            REG_SERVER_LISTEN_PORT
        );
        let master_addr = SockAddr::from(master_addr);
        let mut rng = thread_rng();

        for i in 0..REGISTRATION_RETRIES+1 {
            let sock = Master::new_registration_socket()?;
            match sock.connect_timeout(&master_addr, conn_timeout) {
                Ok(_) => {
                    /* All good! */
                    return Ok(sock)
                },
                Err(e) => {
                    if i == REGISTRATION_RETRIES {
                        let err_msg = String::from("Failed to connect to registration server");
                        let err = MeshSimError {
                            kind: MeshSimErrorKind::Networking(err_msg),
                            cause: Some(Box::new(e)),
                        };
                        return Err(err);
                    } else {
                        let r: u64 = rng.next_u64() % 10;
                        let sleep = Duration::from_nanos(r * ONE_MILLISECOND_NS);
                        let s = format!("{:?}", &sleep);
                        warn!(&logger, "Connection to registration server timed out"; "attempts"=>i+1, "retry_in" => &s);
                        thread::sleep(sleep);
                    }
                },
            }
        }

        let err_msg = String::from("Failed to connect to registration server. Retries exhausted");
        let err = MeshSimError {
            kind: MeshSimErrorKind::Networking(err_msg),
            cause: None,
        };
        Err(err)
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

    fn process_command(
        com: Option<commands::Commands>,
        ph: Arc<dyn Protocol>,
        finish: &AtomicBool,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        // info!(logger, "Received command: {:?}", &com);
        let com = match com {
            Some(c) => c,
            None => return Ok(())
        };
        match com {
            commands::Commands::Send(destination, data) => {
                info!(logger, "Send command received");
                ph.send(destination, data)?;
            },
            commands::Commands::Finish => {
                info!(logger, "Finish command received");
                finish.store(true, Ordering::SeqCst);
            },
            _ => {
                /* The worker does not support any other commands */
                warn!(logger, "Unsupported command received {:?}", com);
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
    // use super::*;
    use rand::RngCore;
    use std::iter;
    use bincode;
    use std::mem;
    use crate::worker::{MessageHeader, Worker};

    #[test]
    #[ignore]
    //This test is not valid at the moment as the hash is now changing everytime
    // with the current timestamp
    fn test_message_header_hash() {
        let sender = String::from("SENDER");
        let destination = String::from("DESTINATION");
        let mut rng = Worker::rng_from_seed(12345);
        let mut data: Vec<u8> = iter::repeat(0u8).take(64).collect();
        rng.fill_bytes(&mut data[..]);
        let msg = MessageHeader::new(sender, destination, data);

        let hash = msg.get_msg_id();
        assert_eq!(hash, "fd8559921846fd7e962ae3e1c3bc6e2d");

    }

    #[test]
    fn test_header_size() {
        let mut rng = Worker::rng_from_seed(12345);
        
        //Case 1: Empty MessageHeader
        let sender = String::from("");
        let destination = String::from("");
        let mut data: Vec<u8> = iter::repeat(0u8).take(0).collect();
        let mut msg = MessageHeader::new(sender, destination, data);
        let empty_header_size = mem::size_of_val(&msg);
        //Base size of the struct
        assert_eq!(120, empty_header_size);
        //Binary encoded version should always be equal or smaller in size
        let ser_msg = msg.to_vec().expect("Could not serialise message");
        let empty_encoded_header_size = mem::size_of_val(&*ser_msg);
        assert!(empty_header_size >= empty_encoded_header_size);

        //Case 2: Increase the payload, ensure the message size grows proportionally
        data = iter::repeat(0u8).take(50).collect();
        rng.fill_bytes(&mut data[..]);
        msg.payload = data;
        let increased_header_size = mem::size_of_val(&msg);
        //Struct size shouldn't change
        assert_eq!(increased_header_size, empty_header_size);
        //But encoded size should grow proportionally
        let ser_msg = msg.to_vec().expect("Could not serialise message");
        let increased_encoded_header_size = mem::size_of_val(&*ser_msg);
        assert_eq!(increased_encoded_header_size, empty_encoded_header_size + 50);
    }
}

// *****************************
// ******** End tests **********
// *****************************
