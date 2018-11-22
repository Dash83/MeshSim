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
use std::collections::HashMap;

//Sub-modules declaration
///Modules that defines the functionality for the test specification.
pub mod test_specification;

/// Master struct.
/// Main data type of the master module and the starting point for creating a new mesh.
/// The master should always be created using the ::new(TestSpec) method.
#[derive(Debug)]
pub struct Master {
    /// Collection of worker processes the Master controls.
    pub workers : Arc<Mutex<HashMap<String, Child>>>,
    ///Working directory for the master under which it will place the files it needs.
    pub work_dir : String,
    /// Path to the worker binary for experiments.
    pub worker_binary : String,
    /// Collection of available worker configurations that the master may start at any time during
    /// the test.
    pub available_nodes : Arc<HashMap<String, WorkerConfig>>,
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
        let workers = Arc::new(Mutex::new(HashMap::new()));
        let wb = String::from("./worker_cli");
        let an = HashMap::new();
        Master{ workers : workers,
                work_dir : String::from("."),
                worker_binary : wb, 
                available_nodes : Arc::new(an) }
    }

    /// Adds a single worker to the worker vector with a specified name and starts the worker process
    pub fn run_worker<'a>( worker_binary : &'a str, work_dir : &'a str, config : &WorkerConfig) -> Result<Child, MasterError> {
        let worker_name = config.worker_name.clone();
        let file_name = format!("{}.toml", &worker_name);
        let mut file_dir = PathBuf::new();
        file_dir.push(work_dir);
        file_dir.push(&file_name);
        debug!("Writing config file {}.", &file_dir.display());
        let _res = try!(config.write_to_file(file_dir.as_path()));

        //Constructing the external process call
        let mut command = Command::new(worker_binary);
        command.arg("--config");
        command.arg(format!("{}", &file_dir.display()));
        command.arg("--work_dir");
        command.arg(format!("{}", work_dir));

        //Starting the worker process
        info!("Starting worker process {}", &worker_name);
        //debug!("with command {:?}", command);
        let child = try!(command.spawn());
        Ok(child)
    }

    ///Runs the test defined in the specification passed to the master.
    pub fn run_test(&mut self, mut spec : test_specification::TestSpec) -> Result<(), MasterError> {
        info!("Running test {}", &spec.name);
        info!("Test results will be placed under {}", &self.work_dir);

        //Start all workers and add save their child process handle.
        {
            let mut workers = self.workers.lock().unwrap(); // LOCK : GET : WORKERS
            for (_, val) in spec.initial_nodes.iter_mut() {
                let child_handle = try!(Master::run_worker(&self.worker_binary, &self.work_dir, &val));
                workers.insert(val.worker_name.clone(), child_handle);
            }
        } // LOCK : RELEASE : WORKERS

        //Add the available_nodes pool to the master.
        self.available_nodes = Arc::new(spec.available_nodes);
        //debug!("Available nodes: {:?}", &self.available_nodes);

        //Run all test actions.
        let actions = spec.actions.clone();
        let action_handles = try!(self.schedule_test_actions(actions));
 
        //All actions have been scheduled. Wait for all actions to be executed and then exit.
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
                    try!(self.testaction_end_test(time))
                },
                TestActions::AddNode(name, time) => { 
                    try!(self.testaction_add_node(name, time))
                },
                TestActions::KillNode(name, time) => {
                    try!(self.testaction_kill_node(name, time))
                }
            };
            thread_handles.push(action_handle);
        }
        Ok(thread_handles)
    }

    fn testaction_end_test(&self, time : u64) -> Result<JoinHandle<()>, MasterError> {
        let workers_handle = Arc::clone(&self.workers);

        let handle = thread::spawn(move || {
            let test_endtime = Duration::from_millis(time);
            info!("End_Test action: Scheduled for {:?}", &test_endtime);
            thread::sleep(test_endtime);
            info!("End_Test action: Starting");
            let mut workers_handle = workers_handle.lock().unwrap();
            let workers_handle = workers_handle.deref_mut();
            let mut i = 0;
            for (_name, mut handle) in workers_handle {
                info!("Killing worker pid {}", handle.id());
                match handle.kill() {
                    Ok(_) => {
                        info!("Process killed.");
                        i += 1;
                    },
                    Err(_) => info!("Process was not running.")
                }
            }
            info!("End_Test action: Finished. {} processes terminated.", i);
        });
        Ok(handle)
    }

    fn testaction_add_node(&self, name : String, time : u64) -> Result<JoinHandle<()>, MasterError> {
        let available_nodes = Arc::clone(&self.available_nodes);
        let workers = Arc::clone(&self.workers);
        let worker_binary = self.worker_binary.clone();
        let work_dir = self.work_dir.clone();
        
        let handle = thread::spawn(move || {
            let test_endtime = Duration::from_millis(time);
            info!("Add_Node ({}) action: Scheduled for {:?}", &name, &test_endtime);
            thread::sleep(test_endtime);
            info!("Add_Node ({}) action: Starting", &name);

            match available_nodes.get(&name) {
                Some(config) => { 
                     match Master::run_worker(&worker_binary, &work_dir, config) {
                         Ok(child_handle) => { 
                            let mut w = workers.lock().unwrap();
                            w.insert(name, child_handle);
                         },
                         Err(e) => { 
                            error!("Error running worker: {:?}", e);
                         },
                     }
                },
                None => { 
                    warn!("Add_Node ({}) action Failed. Worker configuration not found in available_workers pool.", &name);
                },
            }
        });
        Ok(handle)
    }

    fn testaction_kill_node(&self, name : String, time : u64) -> Result<JoinHandle<()>, MasterError> {
        let workers = Arc::clone(&self.workers);
        
        let handle = thread::spawn(move || {
            let killtime = Duration::from_millis(time);
            info!("Kill_Node ({}) action: Scheduled for {:?}", &name, &killtime);
            thread::sleep(killtime);
            info!("Kill_Node ({}) action: Starting", &name);

            let workers = workers.lock();
            match workers {
                Ok(mut w) => { 
                    if let Some(mut child) = w.get_mut(&name) {
                        match child.kill() {
                            Ok(_) => {
                                let exit_status = child.wait();
                                info!("Kill_Node ({}) action: Process {} killed. Exit status: {:?}", &name, child.id(), exit_status); 
                            },
                            Err(e) => error!("Kill_Node ({}) action: Failed to kill process with error {}", &name, e),
                        }
                    } else {
                        error!("Kill_Node ({}) action: Process not found in Master's collection.", &name);
                    }
                },
                Err(e) => { 
                    error!("Kill_Node ({}) action: Could not obtain lock to workers: {}. Process not killed.", &name, e);
                },
            }
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
        let obj_str = r#"Master { workers: Mutex { data: {} }, work_dir: ".", worker_binary: "./worker_cli", available_nodes: {} }"#;
        assert_eq!(format!("{:?}", m), String::from(obj_str));
    }
}