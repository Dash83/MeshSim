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

use ::worker::{Peer, Worker};
use std::process::{Command, Child};
use std::collections::HashMap;
use self::serde_cbor::ser::*;
use self::serde_cbor::de::*;
use self::rustc_serialize::base64::*;

/// Master struct.
/// Main data type of the master module and the starting point for creating a new mesh.
/// The master should always be created using the ::new(TestSpec) method.
//#[derive(Debug)]
pub struct Master {
    /// Vector of worker processes the Master controls.
    pub workers : HashMap<String, Child>,
}

impl Master {
    /// Constructor for the struct.
    pub fn new() -> Master {
        Master{ workers : HashMap::new()}
    }

    /// Adds a single worker to the worker vector with a specified name.
    pub fn add_worker(&mut self, worker : Worker) {
        let worker_name = worker.me.name.clone();
        let worker_data : Vec<u8> = to_vec(&worker).unwrap();
        let worker_data_encoded = worker_data.to_base64(STANDARD);
        let mut command = Command::new("./worker_cli");
        command.arg("--Worker_Object");
        command.arg(format!("{}", worker_data_encoded));
        println!("Starting worker process {}", worker_name);
        let child = command.spawn();

        match child {
            Ok(c) => { 
                self.workers.insert(worker_name, c);
            },
            Err(e) => { 
                println!("Failed to start child process with error {}", e);
            },
        }
        
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
            c.wait();
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