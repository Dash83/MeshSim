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

use crate::worker::listener::Listener;
use crate::worker::protocols::*;
use crate::worker::radio::*;
use crate::{MeshSimError, MeshSimErrorKind};
use byteorder::{NativeEndian, WriteBytesExt};
use libc::{c_int, nice};

use chrono::TimeZone;
use chrono::Utc;
use rand::{rngs::StdRng, SeedableRng};
use serde_cbor::de::*;
use serde_cbor::ser::*;
use slog::{Key, Logger, Record, Serializer, Value};
use std::collections::{HashMap, HashSet};
use std::io::Write;

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc, MutexGuard, PoisonError};
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
const SYSTEM_THREAD_NICE: c_int = -20; //Threads that need to run with a higher priority will use this
const MAIN_THREAD_PERIOD: u64 = 100; //milliseconds

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
    ///Optional, serialized payload of the message.
    /// It's the responsibility of the underlying protocol to know how to deserialize this payload
    /// into a protocol-specific message.
    payload: Vec<u8>,
    ///A hash value that (semi)uniquely identifies this message
    msg_id: String,
}

impl MessageHeader {
    ///Creates a MessageHeader from a serialized vector of bytes.
    pub fn from_vec(data: Vec<u8>) -> Result<MessageHeader, serde_cbor::Error> {
        let msg: Result<MessageHeader, _> = from_reader(&data[..]);
        msg
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
    ///Simulated or Device operation.
    operation_mode: OperationMode,
    /// The protocol that this Worker should run for this configuration.
    pub protocol: Protocols,
    /// The maximum number of queued packets a worker can have
    packet_queue_size: usize,
    ///Threshold after which a packet is considered stale and dropped.
    ///Expressed in milliseconds.
    stale_packet_threshold: i32,
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
        let finish = AtomicBool::new(false);
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
            .map(|(rx, (tx, out_queue_sender, out_queue_receiver))| {
                let prot_handler = Arc::clone(&prot_handler);
                let logger = self.logger.clone();
                let in_queue_thread_pool = threadpool::Builder::new()
                    .num_threads(WORKER_POOL_SIZE)
                    .build();
                let max_queued_jobs = self.packet_queue_size;
                let stale_packet_threshold = self.stale_packet_threshold;

                thread::spawn(move || {
                    let radio_label: String = rx.get_radio_range().into();
                    info!(logger, "[{}] Listening for messages", &radio_label);
                    loop {
                        let radio_label = radio_label.clone();
                        let rl = radio_label.clone();

                        // Read any new messages over the current radio
                        match rx.read_message() {
                            Some(mut hdr) => {
                                //Store the timestamp when this message was queued
                                hdr.delay = Utc::now().timestamp_nanos();
                                let prot = Arc::clone(&prot_handler);
                                let r_type = rx.get_radio_range();
                                let log = logger.clone();

                                if in_queue_thread_pool.queued_count() >= max_queued_jobs {
                                    // let log_data = ();
                                    // log_handle_message(
                                    //     &log,
                                    //     &hdr,
                                    //     MessageStatus::DROPPED,
                                    //     Some("packet_queue is full"),
                                    //     None,
                                    //     &log_data,
                                    // );
                                    let perf_handle_message_duration = Utc::now().timestamp_nanos() - hdr.delay;
                                    //DO NOT change the order of these fields in the logging function.
                                    //This call mirrors the order of log_handle_message and it changed it might break the processing scripts.
                                    info!(
                                        &log,
                                        "Received message";
                                        "radio"=>&radio_label,
                                        "hops"=>hdr.hops,
                                        "destination"=>&hdr.destination,
                                        "source"=>&hdr.sender,
                                        "reason"=>"packet_queue is full",
                                        "duration"=> perf_handle_message_duration,
                                        "status"=>MessageStatus::DROPPED,
                                        "msg_id"=>&hdr.get_msg_id(),
                                    );
                                    continue;
                                }

                                in_queue_thread_pool.execute(move || {
                                    let radio_label = radio_label.clone();
                                    let ts0 = Utc.timestamp_nanos(hdr.delay);
                                    let perf_in_queued_duration =
                                        Utc::now().timestamp_nanos() - ts0.timestamp_nanos();
                                    info!(
                                        &log,
                                        "in_queued";
                                        "msg_id" => hdr.get_msg_id(),
                                        "duration" => perf_in_queued_duration,
                                        "radio" => &radio_label,
                                    );

                                    if perf_in_queued_duration > (stale_packet_threshold * 1000) as i64 {
                                        warn!(
                                            log,
                                            "Skipping message";
                                            "destination" => &hdr.destination,
                                            "source" => &hdr.sender,
                                            "reason" => "stale packet",
                                            "status" => MessageStatus::DROPPED,
                                            "msg_id" => &hdr.msg_id,
                                            "radio" => &radio_label,
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

                                    // resp_tx.send(outcome).unwrap();
                                });
                                debug!(
                                    logger,
                                    "Queued packets: {}",
                                    in_queue_thread_pool.queued_count()
                                );
                                // info!(logger, "Jobs: {}", thread_pool.queued_count());
                                //                                debug!(
                                //                                    logger,
                                //                                    "Jobs that have paniced: {}",
                                //                                    thread_pool.panic_count()
                                //                                );
                            }
                            None => {
                                /* No new messages. Proceed to the next section */
                            }
                        }

                        // Pop the top pending message from the out_queue (if any) and transmit it
                        let resp = out_queue_receiver.try_recv().ok();
                        if let Some((mut resp_hdr, log_data, out_queue_start)) = resp {
                            let tx_channel = Arc::clone(&tx);
                            let log = logger.clone();
                            // let out_queue_start = Utc::now();

                            //Is the packet stale?
                            if (Utc::now().timestamp_millis() - out_queue_start.timestamp_millis()) > 
                                stale_packet_threshold as i64 {
                                warn!(
                                    log,
                                    "Not transmitting";
                                    "destination" => &resp_hdr.destination,
                                    "source" => &resp_hdr.sender,
                                    "reason" => "stale packet",
                                    "status" => MessageStatus::DROPPED,
                                    "msg_id" => &resp_hdr.msg_id,
                                    "radio" => &rl,
                                );
                                return;
                            }

                            // let log_data = match log_data {
                            //     Some(l) => l,
                            //     None => { 
                            //         warn!(
                            //             log,
                            //             "Log_Data was empty";
                            //             "destination" => &r.destination,
                            //             "source" => &r.sender,
                            //             "status" => MessageStatus::DROPPED,
                            //             "msg_id" => &r.msg_id,
                            //         );
                            //         return;
                            //     },
                            // };

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

        /*
        After spawning the listener threads for each radio, the main thread remain in this loop
        periodically executing the maintenance operations of the protocol handler.

        Ideally, it would also poll stdin for new commands instead of having a dedicated thread for that, but
        I'm not sure there's a non-blocking way to read stdin.

        Also in the future, it would be great if instead of having a dedicated logger, the log macros pushed the
        log records into an mpsc channel that was the written by this thread when not doing maintenance tasks.

        With this design, the master could send a stop command to the worker instead of a SIGTERM signal, at which
        point the worker could kill the radio threads, stop doing maintenance work, and focus on flushing the log
        records before exiting.
        */
        let main_thread_period = Duration::from_millis(MAIN_THREAD_PERIOD);
        loop {
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

            //This condition is never set to true at the moment.
            if finish.load(Ordering::SeqCst) {
                break;
            }
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
                                    error!(logger, "Error executing command: {}", &e);
                                    if let Some(cause) = e.cause {
                                        error!(logger, "Cause: {}", &cause);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(logger, "Error parsing command: {}", e);
                            if let Some(cause) = e.cause {
                                error!(logger, "Cause: {}", &cause);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(logger, "{}", &e);
                    //                    if let Some(cause) = e.cause {
                    //                        error!(logger, "Cause: {}", &cause);
                    //                    }
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
    use rand::RngCore;
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

        // rng.fill_bytes(&mut data[..]);
        // msg.payload = data.clone();
        // let hash = msg.get_msg_id();
        // assert_eq!(hash, "48b9f1d803d6829bcab59056981e6771");

        // rng.fill_bytes(&mut data[..]);
        // msg.payload = data.clone();
        // let hash = msg.get_msg_id();
        // assert_eq!(hash, "a6048051b8727b181a6c69edf820914f");
    }

    //This test will me removed at some point, but it's useful at the moment for the message size calculations
    //that will be needed to do segmentation of large pieces of data for transmission.
    #[ignore]
    #[test]
    fn test_message_size() {
        use std::mem;

        let source = String::from("Foo");
        let destination = String::from("Bar");
        let empty_payload: Vec<u8> = vec![];
        let hdr = MessageHeader::new(source, destination, empty_payload);

        println!("Size of MessageHeader: {}", mem::size_of::<MessageHeader>());
        let serialized_hdr = to_vec(&hdr).expect("Could not serialize");
        let hdr_size = mem::size_of_val(&serialized_hdr);
        println!("Size of a serialized MessageHeader: {}", hdr_size);

        // println!("Size of Peer: {}", mem::size_of::<Peer>());
        // let serialized_peer = to_vec(&self_peer).expect("Could not serialize");
        // let peer_size = mem::size_of_val(&serialized_peer);
        // println!("Size of a Peer instance: {}", peer_size);

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
    }
}

// *****************************
// ******** End tests **********
// *****************************
