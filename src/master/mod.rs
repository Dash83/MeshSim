//! Mesh simulator Master module
//! This module defines the Master struct, which represents the master process 
//! in the Mesh deployment.
//! The master process has the following responsibilities:
//!   1. Read the test case and transform it to run-time parameters.
//!   2. Start the number of processes indicated in the test case.
//!   3. Configure each process with the appropriate parameters.
//!   4. Keep track of the life-time of the processes.
//!   5. Kill any processes required by the test especification.



// Lint options for this module
#![deny(missing_docs,
        trivial_casts, trivial_numeric_casts,
        unsafe_code,
        unstable_features,
        unused_import_braces, unused_qualifications)]

// ****** Dependencies ******
extern crate serde;
extern crate serde_cbor;
extern crate rustc_serialize;

use ::worker::Worker;
use std::process::{Command, Child};
use std::io;
use std::error;
use std::fmt;
use std::collections::HashMap;
use self::serde_cbor::ser::*;
use self::rustc_serialize::hex::*;

/// Master struct.
/// Main data type of the master module and the starting point for creating a new mesh.
/// The master should always be created using the ::new(TestSpec) method.
//#[derive(Debug)]
pub struct Master {
    /// Vector of worker processes the Master controls.
    pub workers : HashMap<String, Child>,
}

/// The error type for the Master. Each variant encapsules the underlying reason for failure.
#[derive(Debug)]
pub enum MasterError {
    /// Errors generated when serializing data using serde.
    Serialization(serde_cbor::Error),
    /// Errors generated when doing IO operations.
    IO(io::Error),
}

impl From<serde_cbor::Error> for MasterError {
    fn from(err : serde_cbor::Error) -> MasterError {
        MasterError::Serialization(err)
    }
}

impl From<io::Error> for MasterError {
    fn from(err : io::Error) -> MasterError {
        MasterError::IO(err)
    }
}

impl fmt::Display for MasterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MasterError::Serialization(ref err) => write!(f, "Serialization error: {}", err),
            MasterError::IO(ref err) => write!(f, "IO error: {}", err),
        }
    }

}

impl error::Error for MasterError {
    fn description(&self) -> &str {
        match *self {
            MasterError::Serialization(ref err) => err.description(),
            MasterError::IO(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            MasterError::Serialization(ref err) => Some(err),
            MasterError::IO(ref err) => Some(err),
        }
    }
}


impl Master {
    /// Constructor for the struct.
    pub fn new() -> Master {
        Master{ workers : HashMap::new()}
    }

    /// Adds a single worker to the worker vector with a specified name and starts the worker process
    pub fn add_worker(&mut self, worker : Worker) -> Result<(), MasterError> {
        //Cloning the worker name to pass it as a CLI argument
        let worker_name = worker.me.name.clone();

        //Serializing the worker data to be passed to the worker CLI
        let worker_data : Vec<u8> = try!(to_vec(&worker));
        let worker_data_encoded = worker_data.to_hex();

        //Constructing the external process call
        let mut command = Command::new("./worker_cli");
        command.arg("--Worker_Object");
        command.arg(format!("{}", worker_data_encoded));
        command.arg("--Process_Name");
        command.arg(format!("{}", worker_name));

        //Starting the worker process
        info!("Starting worker process {}", worker_name);
        let child = try!(command.spawn());
        self.workers.insert(worker_name, child);

        Ok(())
    }
    
    // fn start_test(&mut self) -> Result<String, String> {
    //     //let mut handles = vec![];

    //     //crossbeam::scope(|scope| {
    //     //    for worker in &self.workers {
    //     //        let handle = scope.spawn(move || {
    //     //            worker.start();
    //     //        });
    //     //        handles.push(handle)
    //     //    }
    //     //});
    //     let mut w1 = Worker::new("Worker1".to_string());
    //     let mut w2 = Worker::new("Worker2".to_string());
    //     w2.add_peers(vec![w1.me.clone()]);
    //     self.add_worker(w1);
    //     self.add_worker(w2);

    //     for mut c in &mut self.workers {
    //         c.wait();
    //     }
    //     Ok("All workers finished succesfully".to_string())
    // }

    /// Exported method used by clients of Master. Should be the last method called on any tests
    /// in order to wait for the processes to run.
    pub fn wait_for_workers(&mut self) -> Result<String, String> {
        for c in self.workers.values_mut() {
            let _ = c.wait();
        }
        Ok("All workers finished succesfully".to_string())
    }
}

// *****************************
// ********** Tests ************
// *****************************


#[cfg(test)]
mod tests {
    use super::*;

    //**** Create master with 0 workers ****
    #[ignore]
    #[test]
    fn create_empty_master() {
        panic!("test failed!");
    }

    //**** Create master with 10 workers and start them ****
    #[test]
    fn run_ten_workers() {
        let mut master = Master{ workers : vec![] };
        master.add_workers(10, "Worker".to_string());

        assert_eq!("All workers finished succesfully", master.start_test().unwrap());
    }

    //**** Create master with 10 workers and kill 1 ****
    #[ignore]
    #[test]
    fn create_master_ten_children_kill_one() {
        panic!("test failed!");
    }
    
}