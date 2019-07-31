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
        unstable_features,
        unused_import_braces, unused_qualifications)]

// ****** Dependencies ******
extern crate serde;
extern crate serde_cbor;
extern crate rustc_serialize;
extern crate toml;
extern crate base64;
extern crate rand;
extern crate rusqlite;
extern crate libc;

use crate::worker::worker_config::WorkerConfig;
use crate::worker::radio::SimulatedRadio;
use std::process::{Command, Child, Stdio};
use std::io;
use std::io::{BufRead, Write, BufReader};
use std::error;
use std::fmt;
use self::test_specification::{TestActions, Area};
use std::str::FromStr;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
use std::ops::DerefMut;
use std::path::PathBuf;
use std::collections::{HashMap, HashSet};
use crate::worker::mobility::*;
use self::workloads::SourceProfiles;
use self::rand::{thread_rng, Rng, RngCore};
use self::rand::distributions::{Uniform, Normal};
use self::rusqlite::Connection;
use std::sync::{PoisonError, MutexGuard};
use std::iter;
use ::slog::Logger;
use self::libc::{nice, c_int};

//Sub-modules declaration
///Modules that defines the functionality for the test specification.
pub mod test_specification;
mod workloads;

const RANDOM_WAYPOINT_WAIT_TIME : u64 = 1000; //TODO: This must be parameterized
const SYSTEM_THREAD_NICE : c_int = -20; //Threads that need to run with a higher priority will use this

///Different supported mobility models
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub enum MobilityModels {
    /// Random waypoint model
    RandomWaypoint,
}

impl FromStr for MobilityModels {
    type Err = MasterError;

    fn from_str(s : &str) -> Result<MobilityModels, MasterError> {
        let res = match s.to_uppercase().as_str() {
                "RANDOMWAYPOINT" => MobilityModels::RandomWaypoint,
                _ => {
                    return Err(MasterError::TestParsing(String::from("Invalid mobility model")))
                }
        };
        Ok(res)
    }
}

/// Master struct.
/// Main data type of the master module and the starting point for creating a new mesh.
/// The master should always be created using the ::new(TestSpec) method.
#[derive(Debug)]
pub struct Master {
    /// Collection of worker processes the Master controls.
    pub workers : Arc<Mutex<HashMap<String, (i64,Arc<Mutex<Child>>)>>>,
    ///Working directory for the master under which it will place the files it needs.
    pub work_dir : String,
    /// Path to the worker binary for experiments.
    pub worker_binary : String,
    /// Collection of available worker configurations that the master may start at any time during
    /// the test
    pub available_nodes : Arc<HashMap<String, WorkerConfig>>,
    /// Duration in milliseconds of the test
    pub duration : u64,
    /// Area in meters 
    pub test_area : Area,
    /// Current mobility model (if any)
    pub mobility_model : Option<MobilityModels>,
    /// The logger to be usef by the Master
    pub logger : Logger,
    /// The log file to which the master will log
    pub log_file : String,
}

//region Errors
/// The error type for the Master. Each variant encapsules the underlying reason for failure.
#[derive(Debug)]
pub enum MasterError {
    /// Errors generated when serializing data using serde.
    Serialization(serde_cbor::Error),
    /// Errors generated when doing IO operations.
    IO(io::Error),
    ///Errors generated in the worker module.
    Worker(crate::worker::WorkerError),
    ///Errors when deserializing TOML files.
    TOML(toml::de::Error),
    ///Error produced when Master fails to parse a Test specification.
    TestParsing(String),
    ///Error produced when locking shared objects
    Sync(String),
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

impl<'a> From<PoisonError<MutexGuard<'a, HashMap<String, (i64, Arc<Mutex<Child>>)>>>> for MasterError {
    fn from(err : PoisonError<MutexGuard<'a, HashMap<String, (i64, Arc<Mutex<Child>>)>>>) -> MasterError {
        MasterError::Sync(err.to_string())
    }
}

impl From<crate::worker::WorkerError> for MasterError {
    fn from(err : crate::worker::WorkerError) -> MasterError {
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
            MasterError::Sync(ref err) => write!(f, "Sync error: {}", err),
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
            MasterError::Sync(ref err) => err.as_str(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            MasterError::Serialization(ref err) => Some(err),
            MasterError::IO(ref err) => Some(err),
            MasterError::Worker(ref err) => Some(err),
            MasterError::TOML(ref err) => Some(err),
            MasterError::TestParsing(_) => None,
            MasterError::Sync(_) => None,
        }
    }
}

//endregion Errors

impl Master {
    /// Constructor for the struct.
    pub fn new(logger : Logger, log_file : String) -> Master {
        let workers = Arc::new(Mutex::new(HashMap::new()));
        let wb = String::from("./worker_cli");
        let an = HashMap::new();

        Master{ workers : workers,
                work_dir : String::from("."),
                worker_binary : wb, 
                available_nodes : Arc::new(an),
                duration : 0,
                test_area : Area{ width : 0.0, height : 0.0},
                mobility_model : None,
                logger : logger,
                log_file : log_file,
        }
    }

    /// Adds a single worker to the worker vector with a specified name and starts the worker process
    pub fn run_worker<'a>( worker_binary : &'a str, 
                           work_dir : &'a str,
                           listen_for_commands : bool,
                           config : &WorkerConfig,
                           logger : &Logger, ) -> Result<Child, MasterError> {
        let worker_name = config.worker_name.clone();
        debug!(logger, "Starting worker process {}", &worker_name);
        
        let file_name = format!("{}.toml", &worker_name);
        let mut file_dir = PathBuf::new();
        file_dir.push(work_dir);
        file_dir.push(&file_name);
        debug!(logger, "Writing config file {}.", &file_dir.display());
        let _res = config.write_to_file(file_dir.as_path())?;

        //Constructing the external process call
        let mut command = Command::new(worker_binary);
        command.arg("--config");
        command.arg(format!("{}", &file_dir.display()));
        command.arg("--work_dir");
        command.arg(format!("{}", work_dir));
        command.arg("--register_worker");
        command.arg(format!("{}", false));
        command.arg("--accept_commands");
        command.arg(format!("{}", listen_for_commands));
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());

        //Starting the worker process
        //debug!("with command {:?}", command);
        let child = command.spawn()?;
        debug!(logger, "Worker process {} started", &worker_name);

        Ok(child)
    }

    ///Runs the test defined in the specification passed to the master.
    pub fn run_test(&mut self, 
                    mut spec : test_specification::TestSpec,
                    logger : &Logger) -> Result<(), MasterError> {
        info!(self.logger, "Running test {}", &spec.name);
        info!(self.logger, "Test results will be placed under {}", &self.work_dir);

        //Validate all read strings are valid test actions
        let actions = Master::parse_test_actions(spec.actions.clone())?;

        //Get a set of all nodes performing test actions
        let active_nodes = Master::get_active_nodes(&actions);

        //Handles for all the threads that will run test actions
        let mut action_handles = Vec::new();

        //First of all, schedule the End_test action, so that the test will finish even if things go wrong
        let end_test_handle = self.testaction_end_test(self.duration)?;
        action_handles.push(end_test_handle);

        //Obtain database connection
        let conn = get_db_connection(&self.work_dir, logger)?;
        
        //Create the DB objects
        let _rows = create_db_objects(&conn, &logger)?;
        
        //Start all workers and add save their child process handle.
        {
            let mut workers = self.workers.lock()?; // LOCK : GET : WORKERS
            for (_, val) in spec.initial_nodes.iter_mut() {
                //Start the child process
                let listen_for_commands = active_nodes.contains(&val.worker_name);

                let mut res = Master::run_worker(&self.worker_binary, 
                                             &self.work_dir,
                                             listen_for_commands,
                                             &val,
                                             &self.logger);

                match res {
                    Ok(mut child_handle) => {
                        //Get it's listen address from stdout
                        let mut output = String::new();
                        {
                            let mut child_out = BufReader::new(child_handle.stdout.as_mut().unwrap());
                            // eprintln!("stdout acquired");
                            let _num_bytes = child_out.read_line(&mut output)?;
                            // eprintln!("stdout read: {}", &output);
                        }

                        //Register the worker in the DB
                        let worker_id = match val.worker_id {
                            Some(ref id) =>  id.clone(),
                            None => WorkerConfig::gen_id(val.random_seed),
                        };
                        // let sr_addr = SimulatedRadio::format_address(&self.work_dir, &worker_id, RadioTypes::ShortRange);
                        // let lr_addr = SimulatedRadio::format_address(&self.work_dir, &worker_id, RadioTypes::LongRange);
                        let (sr_addr, lr_addr) = SimulatedRadio::extract_radio_addresses(output)?;
                        let res = register_worker(&conn, val.worker_name.clone(), 
                                                                &worker_id, 
                                                                &val.position, 
                                                                &val.velocity, 
                                                                &val.destination,
                                                                sr_addr, 
                                                                lr_addr,
                                                                &logger);
                        
                        match res {
                            Ok(id) => { 
                                //Save the db_id of the worker as well as the handle to its process.
                                workers.insert(val.worker_name.clone(), (id, Arc::new(Mutex::new(child_handle))));
                            },
                            Err(e) => { 
                                error!(&self.logger, "Error registering new worker: {}", e);        
                            },
                        }
                    },
                    Err(e) => { 
                        error!(&self.logger, "Error starting new worker: {}", e);
                    },
                }

            }
        } // LOCK : RELEASE : WORKERS

        //Start mobility thread
        match self.start_mobility_thread() {
            Ok(_h) => { 
                info!(&self.logger, "Mobility thread started");
                //action_handles.push(h);
            },
            Err(e) => { 
                error!(&self.logger, "Failed to start mobility thread: {}", e);
            },
        }

        //Add the available_nodes pool to the master.
        self.available_nodes = Arc::new(spec.available_nodes);
        //debug!("Available nodes: {:?}", &self.available_nodes);

        //Schedule all test actions.
        action_handles.append(&mut self.schedule_test_actions(actions, active_nodes)?);
        info!(&self.logger, "{} test actions scheduled", action_handles.len());

        //let cl = self.start_command_loop_thread()?;

        //All actions have been scheduled. Wait for all actions to be executed and then exit.
        for mut h in action_handles {
             match h.join() {
                Ok(_) => { 

                }, 
                Err(_) => { 
                    warn!(self.logger, "Couldn't join on thread");
                },
             }
        }
        
        //cl.join();

        Ok(())
    }

    fn parse_test_actions(action_strings: Vec<String>) -> Result<Vec<TestActions>, MasterError> {
        let mut actions = Vec::new();
        for a in action_strings {
            let action = TestActions::from_str(&a)?;
            actions.push(action);
        }
        Ok(actions)
    }

    fn get_active_nodes(actions : &Vec<TestActions>) -> HashSet<String> {
        let mut active_nodes = HashSet::new();

        for a in actions {
            match a {
                TestActions::Ping(src, _dst, _time) => {
                    active_nodes.insert(String::from(src.as_str()));
                },
                TestActions::AddSource(src, _profile, _time) => {
                    active_nodes.insert(String::from(src.as_str()));
                },
                _ => {
                    /* No other action requires communicating with the node during the test */
                },
            }
        }

        active_nodes
    }

    fn schedule_test_actions(&self,
                             actions : Vec<TestActions>,
                             active_nodes : HashSet<String> ) -> Result<Vec<JoinHandle<()>>, MasterError> {
        let mut thread_handles = Vec::new();
        
        for action in actions {
            let action_handle = match action {
                TestActions::EndTest(time) => {
                    self.testaction_end_test(time)?
                },
                TestActions::AddNode(name, time) => { 
                    self.testaction_add_node(name.clone(), active_nodes.contains(&name), time)?
                },
                TestActions::KillNode(name, time) => {
                    self.testaction_kill_node(name, time)?
                }
                TestActions::Ping(src, dst, time) => {
                    self.testaction_ping_node(src, dst, time)?
                },
                TestActions::AddSource(src, profile, time) => {
                    self.testaction_add_source(src, profile, time)?
                },
            };
            thread_handles.push(action_handle);
        }
        Ok(thread_handles)
    }

    fn testaction_end_test(&self, time : u64) -> Result<JoinHandle<()>, MasterError> {
        let workers_handle = Arc::clone(&self.workers);
        let logger = self.logger.clone();

        let handle = thread::spawn(move || {
            let test_endtime = Duration::from_millis(time);
            info!(logger, "End_Test action: Scheduled for {:?}", &test_endtime);
            thread::sleep(test_endtime);
            info!(logger, "End_Test action: Starting");
            let mut workers_handle = match workers_handle.lock() { 
                Ok(h) => h,
                Err(e) => { 
                    let msg = format!("Could not acquire lock to list of workers: {}", e);
                    error!(logger, "{}", msg);
                    return
                },
            };
            let workers_handle = workers_handle.deref_mut();
            let mut i = 0;
            for (_name, (_id, handle)) in workers_handle {
                let mut h = handle.lock().unwrap();
                info!(logger, "Killing worker pid {}", h.id());
                match h.kill() {
                    Ok(_) => {
                        info!(logger, "Process killed.");
                        i += 1;
                    },
                    Err(_) => info!(logger, "Process was not running.")
                }
            }
            info!(logger, "End_Test action: Finished. {} processes terminated.", i);
        });
        Ok(handle)
    }

    fn testaction_add_node(&self, name : String,
                                  accept_commands : bool,
                                  time : u64) -> Result<JoinHandle<()>, MasterError> {
        let available_nodes = Arc::clone(&self.available_nodes);
        let workers = Arc::clone(&self.workers);
        let worker_binary = self.worker_binary.clone();
        let work_dir = self.work_dir.clone();
        let logger = self.logger.clone();
        let conn = get_db_connection(&work_dir, &logger)?;

        let handle = thread::spawn(move || {
            let test_endtime = Duration::from_millis(time);
            info!(logger, "Add_Node ({}) action: Scheduled for {:?}", &name, &test_endtime);
            thread::sleep(test_endtime);
            info!(logger, "Add_Node ({}) action: Starting", &name);

            match available_nodes.get(&name) {
                Some(config) => { 
                    let worker_id = match config.worker_id {
                        Some(ref id) =>  id.clone(),
                        None => WorkerConfig::gen_id(config.random_seed),
                    };
                     match Master::run_worker(&worker_binary, &work_dir, accept_commands, config, &logger) {
                        Ok(mut child_handle) => { 
                            //Get it's listen address from stdout
                            let mut output = String::new();
                            {
                                let mut child_out = BufReader::new(child_handle.stdout.as_mut().unwrap());
                                // eprintln!("stdout acquired");
                                let _num_bytes = match child_out.read_line(&mut output) {
                                    Ok(bytes) => bytes,
                                    Err(e) => { 
                                        let msg = format!("Could not read stdout from worker: {}", e);
                                        error!(logger, "{}", msg);
                                        return;
                                    },
                                };
                                // eprintln!("stdout read: {}", &output);
                            }

                            //The process was started correctly. Now register it to the DB.
                            // let sr_addr = SimulatedRadio::format_address(&work_dir, &worker_id, RadioTypes::ShortRange);
                            // let lr_addr = SimulatedRadio::format_address(&work_dir, &worker_id, RadioTypes::LongRange);
                            let (sr_addr, lr_addr) = match SimulatedRadio::extract_radio_addresses(output) {
                                Ok(addresses) => addresses,
                                Err(e) => {
                                    let msg = format!("Could not extract addresses from output: {}", e);
                                    error!(logger, "{}", msg);
                                    return;                                    
                                }
                            };
                            let id = register_worker(&conn, config.worker_name.clone(), 
                                                            &worker_id, 
                                                            &config.position, 
                                                            &config.velocity, 
                                                            &config.destination,
                                                            sr_addr, 
                                                            lr_addr,
                                                            &logger).expect("Failed to register worker in the DB.");
                            let mut w = match workers.lock() { 
                                Ok(h) => h,
                                Err(e) => { 
                                    let msg = format!("Could not acquire lock to list of workers: {}", e);
                                    error!(logger, "{}", msg);
                                    return
                                },
                            };
                            w.insert(name, (id, Arc::new(Mutex::new(child_handle))));
                        },
                        Err(e) => { 
                            error!(logger, "Error running worker: {:?}", e);
                        },
                     }
                },
                None => { 
                    warn!(logger, "Add_Node ({}) action Failed. Worker configuration not found in available_workers pool.", &name);
                },
            }
        });
        Ok(handle)
    }

    fn testaction_kill_node(&self, name : String, time : u64) -> Result<JoinHandle<()>, MasterError> {
        let workers = Arc::clone(&self.workers);
        let logger = self.logger.clone();

        let handle = thread::spawn(move || {
            let killtime = Duration::from_millis(time);
            info!(logger, "Kill_Node ({}) action: Scheduled for {:?}", &name, &killtime);
            thread::sleep(killtime);
            info!(logger, "Kill_Node ({}) action: Starting", &name);

            let workers = workers.lock();
            match workers {
                Ok(mut w) => { 
                    if let Some(mut child) = w.get_mut(&name) {
                        let mut c = child.1.lock().unwrap();
                        match c.kill() {
                            Ok(_) => {
                                let exit_status = c.wait();
                                info!(logger, "Kill_Node ({}) action: Process {} killed. Exit status: {:?}", &name, c.id(), exit_status); 
                            },
                            Err(e) => error!(logger, "Kill_Node ({}) action: Failed to kill process with error {}", &name, e),
                        }
                    } else {
                        error!(logger, "Kill_Node ({}) action: Process not found in Master's collection.", &name);
                    }
                },
                Err(e) => { 
                    error!(logger, "Kill_Node ({}) action: Could not obtain lock to workers: {}. Process not killed.", &name, e);
                },
            }
        });
        Ok(handle)
    }

    fn testaction_ping_node(&self, source : String, destination : String, time : u64) -> Result<JoinHandle<()>, MasterError> {
        let workers = Arc::clone(&self.workers);
        let logger = self.logger.clone();

        let handle = thread::spawn(move || {
            let pingtime = Duration::from_millis(time);
            info!(logger, "Ping {}->{} action: Scheduled for {:?}", &source, &destination, &pingtime);
            thread::sleep(pingtime);
            info!(logger, "Ping {}->{} action: Starting", &source, &destination);

            let workers = workers.lock();
            match workers {
                Ok(mut w) => { 
                    if let Some(mut child) = w.get_mut(&source) {
                        let mut c = child.1.lock().unwrap();
                        let ping_data = base64::encode("PING".as_bytes());
                        let payload = format!("SEND {} {}\n", &destination, &ping_data);
                        let _res = c.stdin.as_mut().unwrap().write_all(payload.as_bytes());
                    } else {
                        error!(logger, "Ping {}->{} action: Process {} not found in Master's collection.", &source, &destination, &source);
                    }
                },
                Err(_e) => { 
                    error!(logger, "Ping {}->{} action: Could not obtain lock to workers, Action aborted.", &source, &destination);
                },
            }
        });
        Ok(handle)
    }

    fn testaction_add_source(&self, source: String, profile : SourceProfiles, time : u64) ->  Result<JoinHandle<()>, MasterError> {
        let tb = thread::Builder::new();
        let start_time = Duration::from_millis(time);
        let workers = Arc::clone(&self.workers);
        let logger = self.logger.clone();

        let handle = tb.name(format!("[Source]:{}", &source))
        .spawn(move || {
            unsafe {
                let new_nice = nice(SYSTEM_THREAD_NICE);
                debug!(logger, "[Source]:{}: New priority: {}", &source, new_nice);
            }
            info!(logger, "[Source]:{}: Scheduled to start in {:?}", &source, &start_time);
            thread::sleep(start_time);

            match profile {
                SourceProfiles::CBR(dest, pps, size, dur) => {
                    let _res = Master::start_cbr_source(source, dest, pps, size, dur, workers, &logger);
                }
            }
        })?;

        Ok(handle)
    }

    fn start_cbr_source(source : String,
                        destination : String, 
                        packets_per_second : usize, 
                        packet_size : usize,
                        duration : u64,
                        workers : Arc<Mutex<HashMap<String, (i64, Arc<Mutex<Child>>)>>>,
                        logger : &Logger) -> Result<(), MasterError> {
        let dur = Duration::from_millis(duration);
        info!(logger, "[Source]:{}: Starting. Will run for {}.{} seconds", &source, &dur.as_secs(), &dur.subsec_nanos());
        info!(logger, "[Source]:{}: Sending {} packets per second of size {} bytes", &source, packets_per_second, packet_size);

        let mut rng = thread_rng();
        let mut data : Vec<u8>= iter::repeat(0u8).take(packet_size).collect();
        let iter_threshold = 1000_000_000u32 / packets_per_second as u32; //In nanoseconds
        let mut packet_counter : u64 = 0;
        let source_handle : Arc<Mutex<Child>> = match workers.lock() {
            Ok(worker_list) => { 
                if let Some(w) = worker_list.get(&source) {
                    Arc::clone(&w.1)
                } else {
                    return Err(MasterError::Sync(format!("Could not find process for {}", &source)))
                }
            },
            Err(_e) => { 
                return Err(MasterError::Sync(String::from("Could not lock workers list")))
            },
        };
        let trans_time = Instant::now();
        let mut iteration = Instant::now();

        while trans_time.elapsed() < dur {
//            let iteration_start = Instant::now();
//            let e = trans_time.elapsed();
//            info!(logger, "[Source]:{}: Running for {}.{} seconds", &source, &e.as_secs(), &e.subsec_nanos());

            packet_counter += 1;

            rng.fill_bytes(&mut data[..]);
            let encoded_data = base64::encode(&data);
            let payload = format!("SEND {} {}\n", &destination, &encoded_data);

//            info!(logger, "[Source]:{}: acquiring lock", &source);
            match source_handle.lock() {
                Ok(mut h) => {
//                    info!(logger, "[Source]:{}: lock acquired", &source);
                    match h.stdin.as_mut() {
                        Some(stdin) => {
                            match stdin.write_all(payload.as_bytes()) {
                                Ok(()) => { 
                                    /* All good */
                                    info!(logger, "Send command {} written successfully", packet_counter);
                                },
                                Err(e) => error!(logger, "Error: {}", e),
                            }
                        },
                        None => { 
                            error!(logger, "Could not get mutable reference to stdin"; "node" => &source);
                        },
                    }
                }
                Err(_e) => {
                    let err = format!("Process {} is not in the Master list. Can't send data. Aborting CBR.", &source);
                    error!(logger, "{}", &err);
                    return Err(MasterError::Sync(err))
                }
            }
            //Calculating pause time
            let iter_duration = iteration.elapsed().subsec_nanos();
            let pause_time = std::cmp::max(iter_threshold -
                                               (std::cmp::max(iter_duration as i32 - iter_threshold as i32, 0i32)) as u32, 0u32);
            iteration = Instant::now();
            let pause = Duration::from_nanos(pause_time as u64);
            debug!(logger, "[Source]:{} sleeping for {}.{} seconds", &source, &pause.as_secs(), &pause.subsec_nanos());
            thread::sleep(pause);
        }
        info!(logger, "[Source]:{}: Finished", &source);
        Ok(())
    }

    fn start_mobility_thread(&self) -> io::Result<JoinHandle<Result<(), MasterError>>> {
        let tb = thread::Builder::new();
        let update_time = Duration::from_millis(1000); //All velocities are expresed in meters per second.
        let sim_start_time = Instant::now();
        let sim_end_time = Duration::from_millis(self.duration);
        let width = self.test_area.width;
        let height = self.test_area.height;
        let duration = self.duration;
        let db_path = self.work_dir.clone();
        let m_model = self.mobility_model.clone();
        let logger = self.logger.clone();

        let conn = match get_db_connection(&self.work_dir, &logger) {
            Ok(c) => c,
            //This is a gross workaround for the error handling but it works for now. 
            //Clean up later. Or never. Probably never.
            Err(_e) => return Err(io::Error::new(io::ErrorKind::Other, MasterError::Sync(String::from("Could not connect to DB"))))
        };

        tb.name(String::from("MobilityThread"))
        .spawn(move || { 
            let mut rng = thread_rng();
            let width_sample = Uniform::new(0.0, width);
            let height_sample = Uniform::new(0.0, height);
            let walking_sample = Normal::new(HUMAN_SPEED_MEAN, HUMAN_SPEED_STD_DEV);

            if let Some(model) = &m_model {
                while Instant::now().duration_since(sim_start_time) < sim_end_time {
                    //Wait times will be uniformly sampled from the remain simulation time.
                    let mut lower_bound: u64 = sim_start_time.elapsed().as_secs() * 1000;
                    lower_bound += sim_start_time.elapsed().subsec_millis() as u64;
                    let wait_sample = Uniform::new(lower_bound, duration);

                    thread::sleep(update_time);

                    //Update worker positions
                    match update_worker_positions(&conn) {
                        Ok(rows) => {
                            debug!(logger, "The position of {} workers has been updated", rows);
                        },
                        Err(e) => {
                            error!(logger, "Error updating worker positions: {}", e);
                        },
                    }

                    // ****** DEBUG ******* //
                    // let workers = get_all_worker_positions(&conn)?;
                    // for w in workers {
                    //     debug!(logger, "{}({}): ({}, {})", w.1, w.0, w.2, w.3);
                    // }

                    match model {
                        MobilityModels::RandomWaypoint => {
                            Master::handle_random_waypoint(&conn,
                                                           &width_sample,
                                                           &height_sample,
                                                           &walking_sample,
                                                           &wait_sample,
                                                           &mut rng,
                                                           &db_path,
                                                           &logger);
                        },
                    }
                }
            } else {
                //No mobility model defined
                info!(logger, "No mobility model defined. Exiting mobility thread.");
                return Ok(())
            }

            Ok(())
        })
    }

    fn handle_random_waypoint(conn : &Connection,
                              width_sample : &Uniform<f64>,
                              height_sample : &Uniform<f64>,
                              walking_sample : &Normal,
                              _wait_sample : &Uniform<u64>,
                              rng : &mut RngCore,
                              db_path : &String,
                              logger : &Logger,  ) {
        debug!(logger, "Entered function handle_random_waypoint");

        //Select workers that reached their destination
        let rows = match select_final_positions(&conn) {
            Ok(rows) => {
                rows
            },
            Err(e) => { 
                error!(logger, "Error updating worker positions: {}", e);
                vec![]
            },
        };

        if rows.len() > 0 {
            info!(logger, "{} workers have reached their destinations", rows.len());
            //Stop workers that have reached their destination
            match stop_workers(&conn, &rows, logger) {
                Ok(r) => {
                    info!(logger, "{} workers have been stopped", r);
                },
                Err(e) => {
                    error!(logger, "Could not stop workers: {}", e);
                }
            }

            for w in rows {
                let w_id = w.0;
                let current_x = w.1;
                let current_y = w.2;

                //Calculate parameters
                // let pause_time :u64 = rng.sample(wait_sample); RANDOM_WAYPOINT_WAIT_TIME
                let pause_time = RANDOM_WAYPOINT_WAIT_TIME;
                let next_x : f64 = rng.sample(width_sample);
                let next_y : f64 = rng.sample(height_sample);
                let vel : f64 = rng.sample(walking_sample);
                let distance : f64 = euclidean_distance(current_x, current_y, next_x, next_y);
                let time : f64 = distance / vel;
                let x_vel = (next_x - current_x) / time;
                let y_vel = (next_y - current_y) / time;

                //Update worker target position
                let _res = update_worker_target(&conn, w_id, Position{x : next_x, y : next_y}, &logger);

                //Schedule thread
                let _h = Master::schedule_new_worker_velocity(pause_time, 
                                                              w_id,
                                                              db_path.clone(),
                                                              Velocity{x : x_vel,  y : y_vel},
                                                              &logger);
            }
        }
    }
    
    fn schedule_new_worker_velocity(pause_time : u64, 
                                    worker_id : i64,
                                    db_path : String,
                                    velocity : Velocity,
                                    logger : &Logger,  ) -> JoinHandle<()> {
        let the_logger =logger.clone();
        let handle = thread::spawn(move || {
            let dur = Duration::from_millis(pause_time);
            info!(the_logger, "Worker_id {} will wait for {}ms", worker_id, pause_time);
            thread::sleep(dur);
            if let Ok(conn) = get_db_connection(&db_path, &the_logger) {
                let _res = match update_worker_vel(&conn, &velocity, worker_id, &the_logger) {
                    Ok(r) => { r },
                    Err(e) => { 
                        error!(the_logger, "Failed updating target for worker_id {}: {}", worker_id, e);
                        0
                    },
                };
            } else {
                error!(the_logger, "Failed updating target for worker_id {}: Could not connect to DB", worker_id);
            }
        });
        handle
    }

//region command_loop
    // fn start_command_loop_thread(&self) -> io::Result<JoinHandle<Result<(), MasterError>>> {
    //     let tb = thread::Builder::new();
    //     let workers = Arc::clone(&self.workers);

    //     tb.name(String::from("CommandLoop"))
    //     .spawn(move || { 
    //         let mut input = String::new();
    //         debug!("Command loop started");
    //         loop {
    //             match io::stdin().read_line(&mut input) {
    //                 Ok(_bytes) => {
    //                     //debug!("Read {} bytes from stdin: {}", _bytes, &input);
    //                     // match input.parse::<commands::Commands>() {
    //                     //     Ok(command) => {
    //                     //         info!("Command received: {:?}", &command);
    //                     //         match Worker::process_command(command, Arc::clone(&protocol_handler)) {
    //                     //             Ok(_) => { /* All good! */ },
    //                     //             Err(e) => {
    //                     //                 error!("Error executing command: {}", e);
    //                     //             }
    //                     //         }
    //                     //     },
    //                     //     Err(e) => { 
    //                     //         error!("Error parsing command: {}", e);
    //                     //     },
    //                     // }
    //                     let workers = workers.lock();
    //                     match workers {
    //                         Ok(mut w_list) => { 
    //                             for (name, mut handle) in w_list.iter_mut() {
    //                                 let res = handle.stdin.as_mut().unwrap().write_all(&input.as_bytes());
    //                             }
    //                         },
    //                         Err(e) => { 
    //                             error!("Could not obtain lock to workers.");
    //                         },
    //                     }
    //                 }
    //                 Err(error) => { 
    //                     error!("{}", error);
    //                 }
    //             }
    //         }
    //         Ok(())
    //     })
    // }
//endregion command_loop
}

// *****************************
// ********** Tests ************
// *****************************
#[cfg(test)]
mod tests {
    

    // //**** Create master with 0 workers ****
    // //Unit test for: Master::new
    // #[test]
    // fn test_master_new() {
    //     let m = Master::new();
    //     let obj_str = r#"Master { workers: Mutex { data: {} }, work_dir: ".", worker_binary: "./worker_cli", available_nodes: {}, duration: 0, test_area: Area { width: 0.0, height: 0.0 }, mobility_model: None }"#;
    //     assert_eq!(format!("{:?}", m), String::from(obj_str));
    // }
}