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
extern crate toml;

use worker::worker_config::WorkerConfig;
use std::process::{Command, Child};
use std::io;
use std::error;
use std::fmt;
use self::test_specification::TestActions;
use std::str::FromStr;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::ops::DerefMut;
use std::path::PathBuf;

//Sub-modules declaration
///Modules that defines the functionality for the test specification.
pub mod test_specification;

/// Master struct.
/// Main data type of the master module and the starting point for creating a new mesh.
/// The master should always be created using the ::new(TestSpec) method.
#[derive(Debug)]
pub struct Master {
    /// Vector of worker processes the Master controls.
    pub workers : Arc<Mutex<Vec<Child>>>,
    ///Working directory for the master under which it will place the files it needs.
    pub work_dir : String,
    /// Path to the worker binary for experiments.
    pub worker_binary : String,
}

/// The error type for the Master. Each variant encapsules the underlying reason for failure.
#[derive(Debug)]
pub enum MasterError {
    /// Errors generated when serializing data using serde.
    Serialization(serde_cbor::Error),
    /// Errors generated when doing IO operations.
    IO(io::Error),
    ///Errors generated in the worker module.
    Worker(::worker::WorkerError),
    ///Errors when deserializing TOML files.
    TOML(toml::de::Error),
    ///Error produced when Master fails to parse a Test specification.
    TestParsing(String),
}

impl From<toml::de::Error> for MasterError {
    fn from(err : toml::de::Error) -> MasterError {
        MasterError::TOML(err)
    }
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

impl From<::worker::WorkerError> for MasterError {
    fn from(err : ::worker::WorkerError) -> MasterError {
        MasterError::Worker(err)
    }
}

impl fmt::Display for MasterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MasterError::Serialization(ref err) => write!(f, "Serialization error: {}", err),
            MasterError::IO(ref err) => write!(f, "IO error: {}", err),
            MasterError::Worker(ref err) => write!(f, "Worker error: {}", err),
            MasterError::TOML(ref err) => write!(f, "TOML error: {}", err),
            MasterError::TestParsing(ref err) => write!(f, "Test parsing error: {}", err),
        }
    }

}

impl error::Error for MasterError {
    fn description(&self) -> &str {
        match *self {
            MasterError::Serialization(ref err) => err.description(),
            MasterError::IO(ref err) => err.description(),
            MasterError::Worker(ref err) => err.description(),
            MasterError::TOML(ref err) => err.description(),
            MasterError::TestParsing(ref err) => err.as_str(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            MasterError::Serialization(ref err) => Some(err),
            MasterError::IO(ref err) => Some(err),
            MasterError::Worker(ref err) => Some(err),
            MasterError::TOML(ref err) => Some(err),
            MasterError::TestParsing(_) => None,
        }
    }
}

impl Master {
    /// Constructor for the struct.
    pub fn new() -> Master {
        let wv = Vec::new();
        let worker_vec = Arc::new(Mutex::new(wv));
        let wb = String::from("./worker_cli");
        Master{ workers : worker_vec,
                work_dir : String::from("."),
                worker_binary : wb }
    }

    /// Adds a single worker to the worker vector with a specified name and starts the worker process
    pub fn add_worker(&mut self, config : WorkerConfig) -> Result<(), MasterError> {
        let worker_name = config.worker_name.clone();
        let file_name = format!("{}.toml", &worker_name);
        let mut file_dir = PathBuf::new();
        file_dir.push(&config.work_dir);
        file_dir.push(&file_name);
        debug!("Writing config file {}.", &file_dir.display());
        let config_file = try!(config.write_to_file(file_dir.as_path()));

        //Constructing the external process call
        let mut command = Command::new(&self.worker_binary);
        command.arg("--config");
        command.arg(format!("{}", config_file));

        //Starting the worker process
        info!("Starting worker process {}", &worker_name);
        //debug!("with command {:?}", command);
        let child = try!(command.spawn());
        //TODO: The unwrap below is used because doing a try! on the result of lock implies
        //I need to implement converting the MutexGuard error to a Master error. I think that's 
        //already implemented in Worker error, but make a note for later.
        //let workers_handle = self.workers.clone();
        //let mut workers_handle = try!(self.workers.lock());
        //workers_handle.push(child);
        self.workers.lock().unwrap().push(child);
        Ok(())
    }

    ///Runs the test defined in the specification passed to the master.
    pub fn run_test(&mut self, mut spec : test_specification::TestSpec) -> Result<(), MasterError> {
        info!("Running test {}", &spec.name);
        info!("Test results will be placed under {}", &self.work_dir);

        //Add all workers to the master. They will be started right away. 
        for (_, val) in spec.nodes.iter_mut() {
            //Since the workers are running as part of the master, change their work_dirs to be relative to the master.
            val.work_dir = self.work_dir.clone();
            /*
            TODO: This 100ms delay between startup of nodes is to avoid potential initialization races. This might be
            removed in the future since it's a bit hacky and the code should be robust enough to handle these issues, but
            it's also a strange situation to have an empty network one instant and the next we have X amount of nodes joining
            at exactly the same time. Will revisit in the future.
            */
            thread::sleep(Duration::from_millis(100));
            try!(self.add_worker(val.clone()));
        }

        //Run all test actions.
        let actions = spec.actions.clone();
        let action_handles = try!(self.schedule_test_actions(actions));
 
        //All actions have been scheduled. Wait for all actions to be executed and then exit.
        //TODO: Make sure the endtest action is scheduled.
        for mut h in action_handles {
             match h.join() {
                Ok(_) => { }, 
                Err(_) => { 
                    warn!("Couldn't join on thread");
                },
             }
        }
        
        Ok(())
    }

    fn schedule_test_actions(&self, actions : Vec<String>) -> Result<Vec<JoinHandle<()>>, MasterError> {
        let mut thread_handles = Vec::new();
        
        for action_str in actions {
            let action = try!(TestActions::from_str(&action_str));
            let action_handle = match action {
                                    TestActions::EndTest(time) => {
                                        let thread_handle = try!(self.testaction_end_test(time));
                                        thread_handle
                                    },
                                };
            thread_handles.push(action_handle);
        }
        Ok(thread_handles)
    }

    fn testaction_end_test(&self, time : u64) -> Result<JoinHandle<()>, MasterError> {
        let workers_handle = self.workers.clone();
        let handle = thread::spawn(move || {
                        let test_endtime = Duration::from_millis(time);
                        info!("EndTest action: Scheduled for {:?}", &test_endtime);
                        thread::sleep(test_endtime);
                        info!("EndTest action: Starting");
                        let mut workers_handle = workers_handle.lock().unwrap();
                        let workers_handle = workers_handle.deref_mut();
                        let mut i = 0;
                        for mut handle in workers_handle {
                            info!("Killing worker pid {}", handle.id());
                            match handle.kill() {
                                Ok(_) => {
                                    info!("Process killed.");
                                    i += 1;
                                },
                                Err(_) => info!("Process was not running.")
                            }
                        }
                        info!("EndTest action: Finished. {} processes terminated.", i);
                    });
        Ok(handle)
    }
}

// *****************************
// ********** Tests ************
// *****************************
#[cfg(test)]
mod tests {
    use super::*;

    //**** Create master with 0 workers ****
    //Unit test for: Master::new
    #[test]
    fn test_master_new() {
        let m = Master::new();
        let obj_str = r#"Master { workers: Mutex { data: [] }, work_dir: ".", worker_binary: "./worker_cli" }"#;
        assert_eq!(format!("{:?}", m), String::from(obj_str));
    }
}