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
        missing_debug_implementations, missing_copy_implementations,
        trivial_casts, trivial_numeric_casts,
        unsafe_code,
        unstable_features,
        unused_import_braces, unused_qualifications)]

// ****** Dependencies ******
extern crate crossbeam;

use ::worker::Worker;
use std::thread;
use std::sync::Arc;

/// Master struct.
/// Main data type of the master module and the starting point for creating a new mesh.
/// The master should always be created using the ::new(TestSpec) method.
#[derive(Debug)]
pub struct Master {
    /// Vector of worker processes the Master controls.
    pub workers : Vec<Worker>
}

impl Master {
    /// Adds a single worker to the worker vector with a specified name.
    fn add_worker(&mut self, name : String) {
        self.workers.push(Worker{ name: name})
    }

    /// Adds a ```num amount of workers to the worker vector.
    /// All workers will be named according the pattern ```name_patternN, where N is a number between 0 and ```num.
    fn add_workers(&mut self, num: usize, name_pattern: String) {
        for i in 0..num {
            self.add_worker(name_pattern.clone() + &i.to_string());
        }
    }

    /// Calls the method start() on all workers.
    //fn run_test(&mut self) {
    //    let data = Arc::new(&self.workers);
    //    let mut handles = vec![];

    //    for i in 0..self.workers.len() {
    //        let local_data = data.clone();
    //        let b = thread::Builder::new().name(local_data[i].name.clone());
    //        handles.push(b.spawn(move || {
    //            local_data[i].start();
    //        }));
    //    }
    //}
    
    fn run_test(&mut self) -> Result<String, String> {
        let mut handles = vec![];

        crossbeam::scope(|scope| {
            for worker in &self.workers {
                let handle = scope.spawn(move || {
                    worker.start();
                });
                handles.push(handle)
            }
        });
        
        for h in handles {
            h.join();
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

    //**** Create master with 1 worker ****
    #[test]
    fn create_master_one_worker() {
        let mut master = Master{ workers : vec![] };
        master.add_worker("The worker".to_string());
        assert_eq!("The worker", master.workers[0].name);
    }

    //**** Create master with 10 workers ****
    #[test]
    fn create_master_ten_workers() {
        let mut master = Master{ workers : vec![] };
        master.add_workers(10, "Worker".to_string());

        let mut i = 0;
        for w in master.workers {
            assert_eq!("Worker".to_string() + &i.to_string(), w.name);
            i += 1;
        }
    }

    //**** Create master with 10 workers and start them ****
    #[test]
    #[ignore]
    fn run_ten_workers() {
        let mut master = Master{ workers : vec![] };
        master.add_workers(10, "Worker".to_string());

        assert_eq!("All workers finished succesfully", master.run_test().unwrap());
    }

    //**** Create master with 10 workers and kill 1 ****
    #[ignore]
    #[test]
    fn create_master_ten_children_kill_one() {
        panic!("test failed!");
    }
    
}