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
extern crate base64;
extern crate rand;
extern crate rusqlite;

use worker::worker_config::WorkerConfig;
use std::process::{Command, Child, Stdio};
use std::io;
use std::error;
use std::fmt;
use self::test_specification::{TestActions, Area};
use std::str::FromStr;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
use std::ops::DerefMut;
use std::path::PathBuf;
use std::collections::HashMap;
use std::io::Write;
use worker::mobility::*;
use self::workloads::SourceProfiles;
use self::rand::{thread_rng, Rng, RngCore};
use self::rand::distributions::{Uniform, Normal};
use self::rusqlite::Connection;
use std::sync::{PoisonError, MutexGuard};
use std::iter;
       
//Sub-modules declaration
///Modules that defines the functionality for the test specification.
pub mod test_specification;
mod workloads;

const RANDOM_WAYPOINT_WAIT_TIME : u64 = 1000; //TODO: This must be parameterized

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
    pub workers : Arc<Mutex<HashMap<String, Arc<Mutex<Child>>>>>,
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
    pub mobility_model : Option<MobilityModels>
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

impl<'a> From<PoisonError<MutexGuard<'a, HashMap<String, Arc<Mutex<Child>>>>>> for MasterError {
    fn from(err : PoisonError<MutexGuard<'a, HashMap<String, Arc<Mutex<Child>>>>>) -> MasterError {
        MasterError::Sync(err.to_string())
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

impl Master {
    /// Constructor for the struct.
    pub fn new() -> Master {
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
        }
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
        command.stdin(Stdio::piped());

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

        //Start mobility thread
        let _mt_h = self.start_mobility_thread()?;

        //Start all workers and add save their child process handle.
        {
            let mut workers = self.workers.lock().unwrap(); // LOCK : GET : WORKERS
            for (_, val) in spec.initial_nodes.iter_mut() {
                let child_handle = try!(Master::run_worker(&self.worker_binary, &self.work_dir, &val));
                workers.insert(val.worker_name.clone(), Arc::new(Mutex::new(child_handle)));
            }
        } // LOCK : RELEASE : WORKERS

        //Add the available_nodes pool to the master.
        self.available_nodes = Arc::new(spec.available_nodes);
        //debug!("Available nodes: {:?}", &self.available_nodes);

        //Add a test action to end the test
        let end_test_action = format!("END_TEST {}\n", &self.duration);
        spec.actions.push(end_test_action);

        //Run all test actions.
        let actions = spec.actions.clone();
        let action_handles = try!(self.schedule_test_actions(actions));
 
        //let cl = self.start_command_loop_thread()?;

        //All actions have been scheduled. Wait for all actions to be executed and then exit.
        for mut h in action_handles {
             match h.join() {
                Ok(_) => { 

                }, 
                Err(_) => { 
                    warn!("Couldn't join on thread");
                },
             }
        }
        
        //cl.join();

        Ok(())
    }

    fn schedule_test_actions(&self, actions : Vec<String>) -> Result<Vec<JoinHandle<()>>, MasterError> {
        let mut thread_handles = Vec::new();
        
        for action_str in actions {
            let action = try!(TestActions::from_str(&action_str));
            let action_handle = match action {
                TestActions::EndTest(time) => {
                    self.testaction_end_test(time)?
                },
                TestActions::AddNode(name, time) => { 
                    self.testaction_add_node(name, time)?
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

        let handle = thread::spawn(move || {
            let test_endtime = Duration::from_millis(time);
            info!("End_Test action: Scheduled for {:?}", &test_endtime);
            thread::sleep(test_endtime);
            info!("End_Test action: Starting");
            let mut workers_handle = workers_handle.lock().unwrap();
            let workers_handle = workers_handle.deref_mut();
            let mut i = 0;
            for (_name, mut handle) in workers_handle {
                let mut h = handle.lock().unwrap();
                info!("Killing worker pid {}", h.id());
                match h.kill() {
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
                            w.insert(name, Arc::new(Mutex::new(child_handle)));
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
                        let mut c = child.lock().unwrap();
                        match c.kill() {
                            Ok(_) => {
                                let exit_status = c.wait();
                                info!("Kill_Node ({}) action: Process {} killed. Exit status: {:?}", &name, c.id(), exit_status); 
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

    fn testaction_ping_node(&self, source : String, destination : String, time : u64) -> Result<JoinHandle<()>, MasterError> {
        let workers = Arc::clone(&self.workers);
        
        let handle = thread::spawn(move || {
            let pingtime = Duration::from_millis(time);
            info!("Ping {}->{} action: Scheduled for {:?}", &source, &destination, &pingtime);
            thread::sleep(pingtime);
            info!("Ping {}->{} action: Starting", &source, &destination);

            let workers = workers.lock();
            match workers {
                Ok(mut w) => { 
                    if let Some(mut child) = w.get_mut(&source) {
                        let mut c = child.lock().unwrap();
                        let ping_data = base64::encode("PING".as_bytes());
                        let payload = format!("SEND {} {}\n", &destination, &ping_data);
                        let res = c.stdin.as_mut().unwrap().write_all(payload.as_bytes());
                    } else {
                        error!("Ping {}->{} action: Process {} not found in Master's collection.", &source, &destination, &source);
                    }
                },
                Err(e) => { 
                    error!("Ping {}->{} action: Could not obtain lock to workers, Action aborted.", &source, &destination);
                },
            }
        });
        Ok(handle)
    }

    fn testaction_add_source(&self, source: String, profile : SourceProfiles, time : u64) ->  Result<JoinHandle<()>, MasterError> {
        let tb = thread::Builder::new();
        let start_time = Duration::from_millis(time);
        let workers = Arc::clone(&self.workers);

        let handle = tb.name(format!("[Source]:{}", &source))
        .spawn(move || { 
            info!("[Source]:{}: Scheduled to start in {:?}", &source, &start_time);
            thread::sleep(start_time);

            match profile {
                SourceProfiles::CBR(dest, pps, size, dur) => {
                    let _res = Master::start_cbr_source(source, dest, pps, size, dur, workers);
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
                        workers : Arc<Mutex<HashMap<String, Arc<Mutex<Child>>>>>) -> Result<(), MasterError> { 
        let mut rng = thread_rng();
        let mut data : Vec<u8>= iter::repeat(0u8).take(packet_size).collect();
        let trans_time = std::time::Instant::now();
        let dur = Duration::from_millis(duration);
        let source_handle : Arc<Mutex<Child>> = match workers.lock() {
            Ok(worker_list) => { 
                if let Some(w) = worker_list.get(&source) {
                    Arc::clone(&w)
                } else {
                    return Err(MasterError::Sync(format!("Could not find process for {}", &source)))
                }
            },
            Err(e) => { 
                return Err(MasterError::Sync(String::from("Could not lock workers list")))
            },
        };

        while trans_time.elapsed() < dur {
            for i in 0..packets_per_second {        
                rng.fill_bytes(&mut data[..]);
                let encoded_data = base64::encode(&data);
                let payload = format!("SEND {} {}\n", &destination, &encoded_data);

                match source_handle.lock() {
                    Ok(mut h) => {
                        let res = h.deref_mut().stdin.as_mut().unwrap().write_all(payload.as_bytes());
                    }
                    Err(e) => {
                        let err = format!("Process {} is not in the Master list. Can't send data. Aborting CBR.", &source);
                        error!("{}", &err);
                        return Err(MasterError::Sync(err))
                    }
                }
                let pause = 1000u64 / packets_per_second as u64;
                thread::sleep(Duration::from_millis(pause));
            }
        }       
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

        let conn = match get_db_connection(&self.work_dir) {
            Ok(c) => c,
            //This is a gross workaround for the error handling but it works for now. 
            //Clean up later. Or never. Probably never.
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, MasterError::Sync(String::from("Could not connect to DB"))))
        };

        tb.name(String::from("MobilityThread"))
        .spawn(move || { 
            let mut rng = thread_rng();
            let width_sample = Uniform::new(0.0, width);
            let height_sample = Uniform::new(0.0, height);
            let walking_sample = Normal::new(HUMAN_SPEED_MEAN, HUMAN_SPEED_STD_DEV);

            while Instant::now().duration_since(sim_start_time) < sim_end_time {
                //Wait times will be uniformilet sampled from the remain simulation time.
                let mut lower_bound : u64 = sim_start_time.elapsed().as_secs() * 1000;
                lower_bound += sim_start_time.elapsed().subsec_millis() as u64;
                let wait_sample = Uniform::new(lower_bound, duration);

                thread::sleep(update_time);

                //Update worker positions
                match update_worker_positions(&conn) {
                    Ok(rows) => { 
                        info!("The position of {} workers has been updated", rows);
                    },
                    Err(e) => { 
                        error!("Error updating worker positions: {}", e);
                    },
                }

                // ****** DEBUG ******* //
                let workers = get_all_worker_positions(&conn)?;
                for w in workers {
                    debug!("{}({}): ({}, {})", w.1, w.0, w.2, w.3);
                }

                if let Some(model) = &m_model {
                    match model {
                        MobilityModels::RandomWaypoint => {
                            Master::handle_random_waypoint( &conn, 
                                                            &width_sample, 
                                                            &height_sample, 
                                                            &walking_sample, 
                                                            &wait_sample, 
                                                            &mut rng, 
                                                            &db_path);
                        },
                    }
                }


            }
            Ok(())
        })
    }

    fn handle_random_waypoint(conn : &Connection,
                              width_sample : &Uniform<f64>,
                              height_sample : &Uniform<f64>,
                              walking_sample : &Normal,
                              wait_sample : &Uniform<u64>,
                              rng : &mut RngCore,
                              db_path : &String  ) {
        //Select workers that reached their destination
        let rows = match select_final_positions(&conn) {
            Ok(rows) => {
                rows
            },
            Err(e) => { 
                error!("Error updating worker positions: {}", e);
                vec![]
            },
        };

        if rows.len() > 0 {
            info!("{} workers have reached their destinations", rows.len());
            //Stop workers that have reached their destination
            match stop_workers(&conn, &rows) {
                Ok(r) => {
                    info!("{} workers have been stopped", r);
                },
                Err(e) => {
                    error!("Could not stop workers: {}", e);
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
                let _res = update_worker_target(&conn, w_id, Position{x : next_x, y : next_y});

                //Schedule thread
                let _h = Master::schedule_new_worker_velocity(pause_time, 
                                                              w_id,
                                                              db_path.clone(),
                                                              Velocity{x : x_vel,  y : y_vel});
            }
        }
    }
    
    fn schedule_new_worker_velocity(pause_time : u64, 
                                    worker_id : i64,
                                    db_path : String,
                                    velocity : Velocity,  ) -> JoinHandle<()> {
        let handle = thread::spawn(move || {
            let dur = Duration::from_millis(pause_time);
            info!("Worker_id {} will wait for {}ms", worker_id, pause_time);
            thread::sleep(dur);
            if let Ok(conn) = get_db_connection(&db_path) {
                let _res = match update_worker_vel(&conn, &velocity, worker_id,) {
                    Ok(r) => { r },
                    Err(e) => { 
                        error!("Failed updating target for worker_id {}: {}", worker_id, e);
                        0
                    },
                };
            } else {
                error!("Failed updating target for worker_id {}: Could not connect to DB", worker_id);
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
    use super::*;

    //**** Create master with 0 workers ****
    //Unit test for: Master::new
    #[test]
    fn test_master_new() {
        let m = Master::new();
        let obj_str = r#"Master { workers: Mutex { data: {} }, work_dir: ".", worker_binary: "./worker_cli", available_nodes: {}, duration: 0, test_area: Area { width: 0.0, height: 0.0 }, mobility_model: None }"#;
        assert_eq!(format!("{:?}", m), String::from(obj_str));
    }
}