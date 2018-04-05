//! Module related to handling and processing the logs produced by the master and worker.


// Lint options for this module
#![deny(missing_docs,
        trivial_casts, trivial_numeric_casts,
        unsafe_code,
        unstable_features,
        unused_import_braces, unused_qualifications)]

extern crate serde_json;

use std::io;
use std::path::Path;
use std::fs::File;
use std::io::BufRead;
use self::serde_json::Value;

struct LogRecord {
    msg : String,
    level : String,
    ts : String,    
}

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
pub fn find_log_record<'a, 'b>(log_key : &'a str, log_value : &'a str, records : &'b Vec<Value>) -> Option<&'b Value> {
    for rec in records {
        if rec[log_key] == log_value {
            return Some(rec)
        }
    }
    None
}