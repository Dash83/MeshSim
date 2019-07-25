//! Module related to handling and processing the logs produced by the master and worker.


// Lint options for this module
#![deny(missing_docs,
        trivial_casts, trivial_numeric_casts,
        unsafe_code,
        unstable_features,
        unused_import_braces, unused_qualifications)]

extern crate serde_json;
extern crate slog_async;
extern crate slog_json;
extern crate slog_term;
extern crate slog_stdlog;

use std::io;
use std::path::Path;
use std::io::BufRead;
use self::serde_json::Value;
use std::fs::{OpenOptions, File};
use ::slog::{Logger, Drain};
use worker::WorkerError;

/// Directory name for where the logs will be placed.
pub const LOG_DIR_NAME : &'static str = "log";
/// Default log file name for the mater process
pub const DEFAULT_MASTER_LOG : &'static str = "Master.log";
const LOG_CHANNEL_SIZE : usize = 512; //Default is 128
const LOG_THREAD_NAME : &'static str = "LoggerThread";

// struct LogRecord {
//     msg : String,
//     level : String,
//     ts : String,    
// }

///Loads a log file and produces an array of log records for processing.
pub fn get_log_records_from_file<P: AsRef<Path>>(path : P) -> Result<Vec<Value>, io::Error> {
    let file = File::open(path)?;
    let mut records = Vec::new();
    let reader = io::BufReader::new(file);

    for line in reader.lines() {
        let data = line?;
        let u : Value = serde_json::from_str(&data)?;
        records.push(u);
    }

    Ok(records)
}

///Given a log key (such as ts) will return the first log record that matches the log_value passed. 
pub fn find_log_record<'a, 'b>(log_key : &'a str,
                               log_value : &'a str,
                               records : &'b Vec<Value>) -> Option<&'b Value> {
    for rec in records {
        if rec[log_key] == log_value {
            return Some(rec)
        }
    }
    None
}

/// Create a duplicate logger for the terminal and the file passed as parameter.
pub fn create_logger<P: AsRef<Path>>(log_file_name : P, log_term : bool ) -> Result<Logger, WorkerError>  {
    //Make sure the full path is valid
    if let Some(parent) = log_file_name.as_ref().parent() {
        let _res = std::fs::create_dir_all(parent).expect("Could not create log directory structure");
    }

    let log_file = OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(log_file_name)?;

    match log_term {
        true => create_term_and_file_logger(log_file),
        false => create_file_logger(log_file)
    }

}

fn create_file_logger(log_file : File) -> Result<Logger, WorkerError> {
    let d2 = slog_json::Json::new(log_file)
        .add_default_keys()
        .build()
        .fuse();
    let d2 = slog_async::Async::new(d2)
        .chan_size(LOG_CHANNEL_SIZE)
        .overflow_strategy(slog_async::OverflowStrategy::Drop)
        .thread_name(format!("File{}", LOG_THREAD_NAME))
        .build().fuse();

    let logger = slog::Logger::root(d2, o!());

    Ok(logger)
}

fn create_term_and_file_logger(log_file : File) -> Result<Logger, WorkerError> {
    //Create the terminal drain
    let decorator = slog_term::TermDecorator::new().build();
    let d1 = slog_term::CompactFormat::new(decorator).build().fuse();
    let d1 = slog_async::Async::new(d1)
        .chan_size(LOG_CHANNEL_SIZE)
        .overflow_strategy(slog_async::OverflowStrategy::Drop)
        .thread_name(format!("Term{}", LOG_THREAD_NAME))
        .build().fuse();

    //Create the file drain
    let d2 = slog_json::Json::new(log_file)
        .add_default_keys()
        .build()
        .fuse();
    let d2 = slog_async::Async::new(d2)
        .chan_size(LOG_CHANNEL_SIZE)
        .overflow_strategy(slog_async::OverflowStrategy::Drop)
        .thread_name(format!("File{}", LOG_THREAD_NAME))
        .build().fuse();

    //Fuse the drains and create the logger
    let logger = slog::Logger::root(slog::Duplicate::new(d1, d2).fuse(), o!());

    Ok(logger)
}

/// Creates a logger that discards all records. Used for tests that don't need logs.
pub fn create_discard_logger() -> Logger {
    slog::Logger::root(slog::Discard, o!())
}