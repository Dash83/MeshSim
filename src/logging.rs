//! Module related to handling and processing the logs produced by the master and worker.

// Lint options for this module
#![deny(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]

use crate::worker::WorkerError;
use crate::mobility::{NodeState, Position, Velocity};
use slog::{Drain, Logger};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::BufRead;
use std::path::Path;
use serde_json::Value;
use std::collections::{HashMap, BTreeMap};
use std::str::FromStr;

/// Directory name for where the logs will be placed.
pub const LOG_DIR_NAME: &str = "log";
/// Default log file name for the mater process
pub const DEFAULT_MASTER_LOG: &str = "Master.log";
const LOG_CHANNEL_SIZE: usize = 512; //Default is 128
const LOG_THREAD_NAME: &str = "LoggerThread";

/// Struct that encapsulates a log entry
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogEntry {
    /// Main log message
    pub msg: String,
    /// Logging level
    pub level: String,
    /// Timestamp of the event
    pub ts: String,
    /// Packet status
    pub status: Option<String>,
    /// Reason for status
    pub reason: Option<String>,
    /// Length of the route that delivered the packet
    pub route_length: Option<usize>,
    /// Number of peers when transmitting
    pub peers_in_range: Option<usize>,
    /// Route to which the operation is associated
    pub route_id: Option<String>,
    /// Type of message
    pub msg_type: Option<String>,
    /// ID of message
    pub msg_id: Option<String>,
}

///Struct to hold a log record of received messages
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReceivedMsgLog {
    /// The timestamp of the message
    pub ts: String,    
    ///The type of message
    pub msg_type: String,
    ///The node that transmitted this message
    pub source: String,
    ///The destination (if any) that this message was meant for
    pub destination: String,
    ///The unique id of this message
    pub msg_id: String,
    ///The number of hops this message took to this destination
    pub hops: u16,
    ///The status of this message. Refer to the *MessageStatus* struct
    pub status: String,
    ///The reason (if any) for the status.
    pub reason: String,
    ///Optional actions triggered by the reception of this message.
    pub action: String,
}

/// Struct to hold a log record of incoming transmissions
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IncomingTxLog {
    /// The timestamp of the message
    pub ts: String,
    /// The stamp of the message
    pub radio: String,
    /// The timestamp of the message
    pub duration: u64,
    /// The timestamp of the message
    pub msg_id: String,
}

///Struct to hold a log record of an outgoing message
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OutgoingMsgLog {
    /// The timestamp of the message
    pub ts: String,
    ///The type of message
    pub msg_type: String,
    ///The node that transmitted this message
    pub source: String,
    ///The destination (if any) that this message was meant for
    pub destination: String,
    ///The unique id of this message
    pub msg_id: String,
    // ///The number of hops this message has taken so far
    // pub hops: u16,
    ///The status of this message. Refer to the *MessageStatus* struct
    pub status: String,
    ///The radio used for the transmission
    pub radio: String,
    // ///The time it took to make the transmission (in nanoseconds)
    // pub duration: String, //TODO: This is a bug. It should be u128, but for some reason radio::log_tx logs it as a string.
    /// Duration (in nanoseconds) the packet spent in the out_queue
    pub out_queued_duration: u64,
    /// Duration (in nanoseconds) the packet spent waiting on medium contention.
    pub contention_duration: u64,
    /// Duration (in nanoseconds) the radio took to transmit the packet to all recipients
    pub tx_duration: u64,
    /// Duration (in nanoseconds) the radio took in the whole broadcast process.
    /// Should be roughly equal to contention_duration + tx_duration
    pub broadcast_duration: u64,
}

/// Logs the state of a node at some point in the simulation
pub fn log_node_state(event: &str, node: NodeState, logger: &Logger) {
    info!(
        logger, 
        "Node-state update";
        "dest"=>node.dest,
        "vel"=>node.vel,
        "pos"=>node.pos,
        "name"=> node.name,
        "id"=>node.id,
        "event"=> event,
    );
}

///Loads a log file and produces an array of log records for processing.
pub fn get_log_records_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<LogEntry>, io::Error> {
    let file = File::open(path)?;
    let mut records = Vec::new();
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let data = line?;
        let u: LogEntry = serde_json::from_str(&data)?;
        records.push(u);
    }

    Ok(records)
}

/// Get records regarding mobility updates in the simulation
pub fn get_mobility_records<P: AsRef<Path>>(path: P) -> Result<HashMap<String, BTreeMap<String, (Position, Velocity)>>, io::Error> {
    let mut records = HashMap::new();
    let file = File::open(path)?;
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let data = line?;
        let u: Value = serde_json::from_str(&data)?;
        if u["msg"] == "Node-state update" {
            let node_name: String = u["name"].to_string().trim_matches(|p| p == '"').to_string();
            let node_records = records
                .entry(node_name)
                .or_insert(BTreeMap::new());
            let ts: String = u["ts"].to_string();
            let pos = u["pos"].to_string();
            let pos = Position::from_str(&pos).expect("Could not parse string into Position");
            let vel = u["vel"].to_string();
            let vel = Velocity::from_str(&vel).expect("Could not parse string into Velocity");
            node_records.insert(ts, (pos, vel));
        }
    }

    Ok(records)
}

///Creates a Vector that holds all the logs of received messages
pub fn get_received_message_records<P: AsRef<Path>>(
    path: P,
) -> Result<Vec<ReceivedMsgLog>, io::Error> {
    let file = File::open(path)?;
    let mut records = Vec::new();
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let data = line?;
        if data.contains("Received message") {
            let u: ReceivedMsgLog = serde_json::from_str(&data)?;
            records.push(u);
        }
    }

    Ok(records)
}

///Creates a Vector that holds all logs of incoming transmissions as they are read from the medium
pub fn get_incoming_transmission_records<P: AsRef<Path>>(
    path: P,
) -> Result<Vec<IncomingTxLog>, io::Error> {
    let file = File::open(path)?;
    let mut records = Vec::new();
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let data = line?;
        if data.contains("read_from_network") {
            let u: IncomingTxLog = serde_json::from_str(&data)?;
            records.push(u);
        }
    }

    Ok(records)
}

///Creates a Vector that holds all the outgoing message logs
pub fn get_outgoing_message_records<P: AsRef<Path>>(
    path: P,
) -> Result<Vec<OutgoingMsgLog>, io::Error> {
    let file = File::open(path)?;
    let mut records = Vec::new();
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let data = line?;
        if data.contains("Message sent") {
            let u: OutgoingMsgLog = serde_json::from_str(&data)?;
            records.push(u);
        }
    }

    Ok(records)
}

///Creates a Vector that holds all last_tranmission message logs
pub fn get_last_transmission_records<P: AsRef<Path>>(
    path: P,
) -> Result<Vec<Value>, io::Error> {
    let file = File::open(path)?;
    let mut records = Vec::new();
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let data = line?;
        if data.contains("last_transmission") {
            let u: Value = serde_json::from_str(&data)?;
            records.push(u);
        }
    }

    Ok(records)
}

///Given a log key (such as ts) will return the first log record that matches the log_value passed.
pub fn find_record_by_msg<'a>(msg: &str, records: &'a [LogEntry]) -> Option<&'a LogEntry> {
    for rec in records {
        if &rec.msg == &msg {
            return Some(rec);
        }
    }
    None
}

/// Create a duplicate logger for the terminal and the file passed as parameter.
pub fn create_logger<P: AsRef<Path>>(
    log_file_name: P,
    log_term: bool,
) -> Result<Logger, WorkerError> {
    //Make sure the full path is valid
    if let Some(parent) = log_file_name.as_ref().parent() {
        std::fs::create_dir_all(parent).expect("Could not create log directory structure");
    }

    let log_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(log_file_name)?;

    if log_term {
        create_term_and_file_logger(log_file)
    } else {
        create_file_logger(log_file)
    }
}

fn create_file_logger(log_file: File) -> Result<Logger, WorkerError> {
    let d2 = slog_json::Json::new(log_file)
        .add_default_keys()
        .build()
        .fuse();
    let d2 = slog_async::Async::new(d2)
        .chan_size(LOG_CHANNEL_SIZE)
        .overflow_strategy(slog_async::OverflowStrategy::Drop)
        .thread_name(format!("File{}", LOG_THREAD_NAME))
        .build()
        .fuse();

    let logger = slog::Logger::root(d2, o!());

    Ok(logger)
}

fn create_term_and_file_logger(log_file: File) -> Result<Logger, WorkerError> {
    //Create the terminal drain
    let decorator = slog_term::TermDecorator::new().build();
    let d1 = slog_term::CompactFormat::new(decorator).build().fuse();
    let d1 = slog_async::Async::new(d1)
        .chan_size(LOG_CHANNEL_SIZE)
        .overflow_strategy(slog_async::OverflowStrategy::Drop)
        .thread_name(format!("Term{}", LOG_THREAD_NAME))
        .build()
        .fuse();

    //Create the file drain
    let d2 = slog_json::Json::new(log_file)
        .add_default_keys()
        .build()
        .fuse();
    let d2 = slog_async::Async::new(d2)
        .chan_size(LOG_CHANNEL_SIZE)
        .overflow_strategy(slog_async::OverflowStrategy::Drop)
        .thread_name(format!("File{}", LOG_THREAD_NAME))
        .build()
        .fuse();

    //Fuse the drains and create the logger
    let logger = slog::Logger::root(slog::Duplicate::new(d1, d2).fuse(), o!());

    Ok(logger)
}

/// Creates a logger that discards all records. Used for tests that don't need logs.
pub fn create_discard_logger() -> Logger {
    slog::Logger::root(slog::Discard, o!())
}
