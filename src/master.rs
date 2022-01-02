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
#![deny(
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unstable_features,
    unused_import_braces,
    unused_qualifications
)]

use crate::backend::*;
use crate::worker::worker_config::WorkerConfig;
use crate::worker::commands::Commands;
use crate::*;
use crate::mobility::*;
use crate::logging::log_node_state;
use chrono::Utc;
use libc::{c_int, nice};

use rand::{RngCore, thread_rng};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::{net::{IpAddr, Ipv6Addr, SocketAddr}};
// use serde_cbor::de::*;
use crossbeam_channel::unbounded;
use std::sync::atomic::{AtomicBool, Ordering};
use sysinfo::{self, ProcessExt, SystemExt, Signal};
use std::path::Path;
use itertools::Itertools;
// use rusqlite::Connection;

use diesel::pg::PgConnection;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::error;
use std::fmt;
use std::io;

use std::iter;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use test_specification::{Area, TestActions};
use workloads::SourceProfiles;
// use serde_cbor::ser::*;
use rand::{rngs::StdRng, SeedableRng};

//Sub-modules declaration
///Modules that defines the functionality for the test specification.
pub mod test_specification;
mod workloads;

/// Port on which the Master listens for workers to register
pub const REG_SERVER_LISTEN_PORT: u16 = 12345;
//Size of the threadpool to handle worker registration.
//A certain level of parallelism is required since most nodes are expected to be started almost
//simultaneously during a simulation.
const REG_SERVER_POOL_SIZE: usize = 2;
const REG_SERVER_PERIOD: u64 = 100 * ONE_MICROSECOND_NS;
/// Maximum number of backlog connections for the registration server. 
/// Ideally, this should be the number of expected nodes, to avoid any registration issues,
/// but this is a sane default for now.
const REG_SERVER_BACKLOG: i32 = 256;
const RANDOM_WAYPOINT_WAIT_TIME: i64 = 1000;
const SYSTEM_THREAD_NICE: c_int = -20; //Threads that need to run with a higher priority will use this
const CLEANUP_SLEEP: u64 = 500 * ONE_MILLISECOND_NS;
/// Default random seed used for the master process
pub const DEFAULT_RANDOM_SEED: u64 = 0;

///Different supported mobility models
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(tag = "Model")]
pub enum MobilityModels {
    /// Random waypoint model
    RandomWaypoint {
        /// The amount of time a node will wait once it reaches its destination.
        pause_time: i64,
        /// The time interval in nanoseconds that the master will update the worker positions. Defaults to MOBILITY_PERIOD.
        period: Option<u64>,
    },
    /// No movement
    Stationary {
        /// The time interval in nanoseconds that the master will update the worker positions. Defaults to MOBILITY_PERIOD.
        period: Option<u64>,
    },
    /// Increased mobility model
    IncreasedMobility {
        /// The mean velocity used for nodes
        vel_mean: f64,
        /// The standard deviation of the velocity
        vel_std: f64,
        /// The percentage increase of velocity used each time a node reaches its destination. Between 0 and 1.
        vel_increase: f64,
        /// The time a node will pause each it reaches its destination.
        pause_time: i64,
        /// The time interval in nanoseconds that the master will update the worker positions. Defaults to MOBILITY_PERIOD.
        period: Option<u64>,
    },
}

impl FromStr for MobilityModels {
    type Err = MeshSimError;

    fn from_str(s: &str) -> Result<MobilityModels, MeshSimError> {
        let parts: Vec<&str> = s.split_whitespace().collect();

        if parts.is_empty() {
            let msg = String::from("Empty mobility model");
            let err = MeshSimError {
                kind: MeshSimErrorKind::TestParsing(msg),
                cause: None,
            };
            return Err(err);
        }

        match parts[0].to_uppercase().as_str() {
            "RANDOMWAYPOINT" => {
                let pause_time = parts[1].parse::<i64>().unwrap_or(RANDOM_WAYPOINT_WAIT_TIME);
                let mut period = None;
                if parts.len() > 2 {
                    period = Some(parts[2].parse::<u64>().unwrap_or(DEFAULT_MOBILITY_PERIOD));
                    if period.unwrap() < MINIMUM_MOBILITY_PERIOD {
                        let err_msg = format!("Invalid value for mobility update period. \nMinimum allowable value is {}", MINIMUM_MOBILITY_PERIOD);
                        let e = MeshSimError{
                            kind: MeshSimErrorKind::Configuration(err_msg),
                            cause: None,
                        };
                        return Err(e);
                    }
                }
                Ok(MobilityModels::RandomWaypoint { pause_time, period })
            }
            "STATIONARY" => {
                let mut period = None;
                if parts.len() > 1 {
                    period = Some(parts[1].parse::<u64>().unwrap_or(DEFAULT_MOBILITY_PERIOD));
                    if period.unwrap() < MINIMUM_MOBILITY_PERIOD {
                        let err_msg = format!("Invalid value for mobility update period. \nMinimum allowable value is {}", MINIMUM_MOBILITY_PERIOD);
                        let e = MeshSimError{
                            kind: MeshSimErrorKind::Configuration(err_msg),
                            cause: None,
                        };
                        return Err(e);
                    }
                }
                Ok(MobilityModels::Stationary{ period })
            }
            "INCREASEDMOBILITY" => {
                if parts.len() < 5 {
                    let err_msg = String::from("Wrong number of arguments for Increased Mobility. \nExpected: VEL_MEAN VEL_STD VEL_INCREASE PAUSE_TIME");
                    let e = MeshSimError{
                        kind: MeshSimErrorKind::Configuration(err_msg),
                        cause: None,
                    };
                    return Err(e);
                }
                let vel_mean = parts[1].parse::<f64>()
                    .map_err(|e| { 
                        let err_msg = String::from("Failed to parse VEL_MEAN for IncreasedMobility model");
                        MeshSimError{
                            kind: MeshSimErrorKind::Configuration(err_msg),
                            cause: Some(Box::new(e)),
                        }
                    })?;
                let vel_std = parts[2].parse::<f64>()
                    .map_err(|e| { 
                        let err_msg = String::from("Failed to parse VEL_STD for IncreasedMobility model");
                        MeshSimError{
                            kind: MeshSimErrorKind::Configuration(err_msg),
                            cause: Some(Box::new(e)),
                        }
                    })?;
                let vel_increase = parts[3].parse::<f64>()
                    .map_err(|e| { 
                        let err_msg = String::from("Failed to parse VEL_INCREASE for IncreasedMobility model");
                        MeshSimError{
                            kind: MeshSimErrorKind::Configuration(err_msg),
                            cause: Some(Box::new(e)),
                        }
                    })?;

                let pause_time = parts[4].parse::<i64>()
                    .map_err(|e| { 
                        let err_msg = String::from("Failed to parse PAUSE_TIME for IncreasedMobility model");
                        MeshSimError{
                            kind: MeshSimErrorKind::Configuration(err_msg),
                            cause: Some(Box::new(e)),
                        }
                    })?;

                let mut period = None;
                if parts.len() > 5 {
                    period = Some(parts[5].parse::<u64>().unwrap_or(DEFAULT_MOBILITY_PERIOD));
                    if period.unwrap() < MINIMUM_MOBILITY_PERIOD {
                        let err_msg = format!("Invalid value for mobility update period. \nMinimum allowable value is {}", MINIMUM_MOBILITY_PERIOD);
                        let e = MeshSimError{
                            kind: MeshSimErrorKind::Configuration(err_msg),
                            cause: None,
                        };
                        return Err(e);
                    }
                }

                Ok(MobilityModels::IncreasedMobility { vel_mean, vel_std, vel_increase, pause_time, period })
            },
            _ => {
                let err_msg = String::from("Invalid mobility model");
                let err = MeshSimError {
                    kind: MeshSimErrorKind::TestParsing(err_msg),
                    cause: None,
                };
                Err(err)
            }
        }
    }
}

/// Represents a child process that the master controls.
type Process = (i32, String);

/// Master struct.
/// Main data type of the master module and the starting point for creating a new mesh.
/// The master should always be created using the ::new(TestSpec) method.
#[derive(Debug)]
pub struct Master {
    /// Collection of worker processes the Master controls.
    pub workers: Arc<Mutex<HashMap<String, Process>>>,
    ///Working directory for the master under which it will place the files it needs.
    pub work_dir: String,
    /// Path to the worker binary for experiments.
    pub worker_binary: String,
    /// Collection of available worker configurations that the master may start at any time during
    /// the test
    pub available_nodes: Arc<HashMap<String, WorkerConfig>>,
    /// Duration in milliseconds of the test
    pub duration: u64,
    /// Area in meters
    pub test_area: Area,
    /// Current mobility model (if any)
    pub mobility_model: Option<MobilityModels>,
    /// Name of the experiment DB that will be created for this run.
    db_name: String,
    /// DB-connection file
    env_file: String,
    /// The logger to be usef by the Master
    pub logger: Logger,
    /// The log file to which the master will log
    pub log_file: String,
    /// Has the sleep_time for workers been overriden in the passed params?
    pub sleep_time_override: Option<u64>,
    /// Registration server handle
    rs_handle: JoinHandle<()>,
    /// Signal across the Master threads that the test has ended so they can wrap up and finish.
    finish_test: Arc<AtomicBool>,
    /// Random seed used to initialise all rngs in the master process.
    random_seed: u64,
    /// Random number generator used by the Master for all internal operations
    rng: StdRng,
}

//region Errors
/// The error type for the Master. Each variant encapsules the underlying reason for failure.
#[derive(Debug)]
pub enum MasterError {
    /// Errors generated when serializing data using serde.
    Serialization(bincode::Error),
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
    fn from(err: toml::de::Error) -> MasterError {
        MasterError::TOML(err)
    }
}

impl From<bincode::Error> for MasterError {
    fn from(err: bincode::Error) -> MasterError {
        MasterError::Serialization(err)
    }
}

impl From<io::Error> for MasterError {
    fn from(err: io::Error) -> MasterError {
        MasterError::IO(err)
    }
}

impl From<crate::worker::WorkerError> for MasterError {
    fn from(err: crate::worker::WorkerError) -> MasterError {
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

impl Error for MasterError {
    // fn description(&self) -> &str {
    //     match *self {
    //         MasterError::Serialization(ref err) => err.description(),
    //         MasterError::IO(ref err) => err.description(),
    //         MasterError::Worker(ref err) => err.description(),
    //         MasterError::TOML(ref err) => err.description(),
    //         MasterError::TestParsing(ref err) => err.as_str(),
    //         MasterError::Sync(ref err) => err.as_str(),
    //     }
    // }

    fn cause(&self) -> Option<&dyn Error> {
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
    pub fn new(work_dir: String, worker_path: String, log_file: String, db_name: String, random_seed: Option<u64>, logger: Logger) -> Result<Master, MeshSimError> {
        let workers = Arc::new(Mutex::new(HashMap::new()));
        let an = HashMap::new();
        let env_file = create_db_objects(&work_dir, &db_name, &logger)?;
        let finish_test = Arc::new(AtomicBool::new(false));
        let random_seed = random_seed.unwrap_or(DEFAULT_RANDOM_SEED);
        let rng = StdRng::seed_from_u64(random_seed);

        debug!(&logger, "Using connection file: {}", &env_file);
        debug!(&logger, "Using DB name: {}", &db_name);

        // Master::start_registration_server(port)?;
        let handle = Master::start_registration_server(
            REG_SERVER_LISTEN_PORT,
            env_file.clone(),
            Arc::clone(&workers),
            Arc::clone(&finish_test),
            logger.clone(),
        )?;

        info!(&logger, "Regisration server started");

        let m = Master {
            workers,
            work_dir,
            worker_binary: worker_path,
            available_nodes: Arc::new(an),
            duration: 0,
            test_area: Area {
                width: 0.0,
                height: 0.0,
            },
            mobility_model: None,
            db_name,
            env_file,
            logger,
            log_file,
            sleep_time_override: None,
            rs_handle: handle,
            finish_test,
            random_seed,
            rng,
        };


        Ok(m)
    }

    fn start_registration_server(
        listening_port: u16,
        env_file: String,
        workers: Arc<Mutex<HashMap<String, (i32, String)>>>,
        finish_test: Arc<AtomicBool>,
        logger: Logger,
    ) -> Result<JoinHandle<()>, MeshSimError> {
        //Do all socket operatons before spawning the threads, so that we can both fail earlier if there's an issue,
        //and can propagate the error back since it gets hairy to do so inside the threads.
        let sock = Master::new_registration_socket()?;
        let base_addr = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
                listening_port
        );
        // The API requires that we first bind the socket to the listen adddress...
        let _ = sock.bind(&SockAddr::from(base_addr))
        .map_err(|e| {
            let err_msg = format!("Could not bind socket to address {}", &base_addr);
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        // and then mark it as ready to listen. Can't call accept on it before these 2 things.
        let _ = sock.listen(REG_SERVER_BACKLOG)
        .map_err(|e| {
            let err_msg = format!("Could not bind socket to address {}", &base_addr);
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        //Finally, set the server socket into nonblocking mode, otherwise it is very likely this thread will hang
        //on the accept() call even if the finish_test signal has been set.
        sock.set_nonblocking(true)
        .map_err(|e| {
            let err_msg = "Failed to set RegistrationServer socket into nonblocking mode".to_string();
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        //Create the queues for the clients
        let (client_queue_sender, client_queue_receiver) = unbounded();

        // start the listener thread
        let j = thread::Builder::new()
        .name(String::from("RegistrationServer"))
        .spawn(move || {
            //Start the workers
            let mut worker_threads = Vec::new();
            let log = logger.clone();
            let conn_file = env_file.clone();

            for i in 0..REG_SERVER_POOL_SIZE {
                //Resources for each thread
                let thread_name = format!("RegWorker{}", i);
                let client_queue = client_queue_receiver.clone();
                let finish_test = Arc::clone(&finish_test);
                let w = Arc::clone(&workers);
                let l = log.clone();
                let cf = conn_file.clone();
                let j = thread::Builder::new()
                    .name(thread_name.clone())
                    .spawn(move || {
                        //Local-scope resources for this worker thread
                        let conn = match get_db_connection_by_file(cf, &l) {
                            Ok(c) => { c } ,
                            Err(_e) => {
                                error!(l, "RegServerWorker failed to obtain database connection and will terminate"; "thread"=>&thread_name);
                                return;
                            }
                        };
                        let read_timeout = Duration::from_nanos(100 * ONE_MILLISECOND_NS);
                        while !finish_test.load(Ordering::SeqCst) {
                            match client_queue.recv_timeout(read_timeout) {
                                Ok((client, client_address)) => {
                                    match Master::handle_client(
                                        client,
                                        client_address,
                                        &conn,
                                        Arc::clone(&w),
                                        &l)
                                    {
                                        Ok(_) => {
                                            /* All good */
                                        },
                                        Err(e) => {
                                            error!(l, "Error handling client: {}", e);
                                        },
                                    }
                                },
                                Err(e) => {
                                    if e.is_timeout() {
                                        /* The read operation timed out, which is totally fine. Moving on. */
                                        continue;
                                    }
                                    error!(l, "{} - Failed to get client from queue {}", &thread_name, &e);
                                },
                            }
                        }
                    })
                    .expect("Failed to spawn RegWorker thread");
                    // .map_err(|e| {
                    //     let err_msg = String::from("Failed to spawn RegWorker thread");
                    //     MeshSimError {
                    //         kind: MeshSimErrorKind::Master(err_msg),
                    //         cause: Some(Arc::new(e)),
                    //     }
                    // })?;
                worker_threads.push(j);
            }

            //Start the listeneing loop
            let sleep_time = Duration::from_micros(REG_SERVER_PERIOD);
            while !finish_test.load(Ordering::SeqCst) {
                let l = log.clone();
                
                let (client, client_address) = match sock.accept() {
                    Ok(data) => {
                        data
                    },
                    Err(e) => {
                        /* If it's a timeout error, just continue to the next iteration. */
                        //Log all other errors though.
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            thread::sleep(sleep_time);
                        } else {
                            error!(l, "[RegistrationServer] - Error accepting new client connection: {}", &e);
                        }
                        continue;
                    }
                };
                match client_queue_sender.send((client, client_address)) {
                    Ok(_) => {
                        /* All good! */
                    },
                    Err(e) => {
                        error!(l, "[RegistrationServer] - Failed to queue new client connection {}", &e);
                    }
                }

            };

            //Wait for the worker threads to finish and log any errors
            for h in worker_threads {
                match h.join() {
                    Ok(_) => {
                        /* All good! */
                    },
                    Err(e) => {
                        error!(log, "RegWorker thread exited with the following error: {:?}", &e);
                    }
                }
            }

        })
        .map_err(|e| {
            let err_msg = String::from("Failed to spawn RegistrationServer thread");
            MeshSimError {
                kind: MeshSimErrorKind::Master(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

        // info!(&logger, "Master has been initialised");
        Ok(j)
    }

    fn handle_client(
        client: Socket,
        client_addr: SockAddr,
        conn: &PgConnection,
        workers: Arc<Mutex<HashMap<String, (i32, String)>>>,
        log: &Logger,
    ) -> Result<(), MeshSimError> {
        //64kb buffer
        let mut buffer = [0; 65536];
        //Set the client socket to non-blocking, otherwise the receive operation would fail.
        client.set_nonblocking(false)
        .map_err(|e| {
            let err_msg = "Failed to set client socket into blocking mode".to_string();
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        //Also set a read timeout, otherwise this thread might hand, and the entire Master would wait on it to finish.
        let read_timeout = Duration::from_nanos(200 * ONE_MILLISECOND_NS);
        client.set_read_timeout(Some(read_timeout))
        .map_err(|e| {
            let err_msg = "Failed to set client read timeout".to_string();
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        let bytes_read = client.recv(&mut buffer)
        .map_err(|e| {
            let err_msg = format!("Failed to read data from client {}", client_addr.as_inet6().unwrap());
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        debug!(log, "Connection received from {}", client_addr.as_inet6().unwrap(); "bytes_read" => bytes_read);

        //Attempt to parse the read data into a Command
        let data = buffer[..bytes_read].to_vec();
        let cmd: Commands = bincode::deserialize(&data)
        .map_err(|e| {
            let err_msg = "Could not deserialise message from client".to_string();
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

        match cmd {
            Commands::RegisterWorker{
                w_name,
                w_id,
                pos,
                vel,
                dest,
                sr_address,
                lr_address,
                cmd_address,} => {

                info!(
                    &log,
                    "Registration command received";
                    "worker" => &w_name,
                );
                let vel = vel.unwrap_or_default();
                let id = register_worker(&conn, w_name.clone(), w_id, pos, vel, &dest, sr_address, lr_address, &log)?;

                //Save the db_id of the worker as well as the handle to its process.
                let mut workers = workers
                    .lock()
                    .expect("Failed to lock workers list");
                workers.insert(w_name.clone(), (id, cmd_address));
                let ns = NodeState { id, name: w_name, pos, vel, dest };
                log_node_state("Worker registered successfully!", ns, &log);
            },
            _ => {
                /* The master does  not support any other commands*/
                warn!(log, "Unsupported command received {:?}", cmd);
            }
        }

        Ok(())
    }

    /// Adds a single worker to the worker vector with a specified name and starts the worker process
    pub fn run_worker<'a>(
        worker_binary: &'a str,
        work_dir: &'a str,
        listen_for_commands: bool,
        config: &WorkerConfig,
        conn_str: &String,
        logger: &Logger,
    ) -> Result<Child, MeshSimError> {
        let worker_name = config.worker_name.clone();
        debug!(logger, "Starting worker process {}", &worker_name);

        let file_name = format!("{}.toml", &worker_name);
        let mut file_dir = PathBuf::new();
        file_dir.push(work_dir);
        file_dir.push(&file_name);
        debug!(logger, "Writing config file {}.", &file_dir.display());
        config.write_to_file(file_dir.as_path())?;

        //Constructing the external process call
        let child = Command::new(worker_binary)
            .env(DB_CONN_ENV_VAR, conn_str)
            .arg("--config")
            .arg(format!("{}", &file_dir.display()))
            .arg("--work_dir")
            .arg(work_dir.to_string())
            .arg("--register_worker")
            .arg(format!("{}", false))
            .arg("--accept_commands")
            .arg(format!("{}", listen_for_commands))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| {
                let err_msg = String::from("Failed to spawn worker process");
                MeshSimError {
                    kind: MeshSimErrorKind::Master(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;
        debug!(logger, "Worker process {} started", &worker_name);

        Ok(child)
    }

    ///Runs the test defined in the specification passed to the master.
    pub fn run_test(
        mut self,
        mut spec: test_specification::TestSpec,
        sleep_time_override: Option<&str>,
    ) -> Result<(), MeshSimError> {
        info!(self.logger, "Running test {}", &spec.name);
        info!(
            self.logger,
            "Test results will be placed under {}", &self.work_dir
        );

        //Validate all read strings are valid test actions
        let actions = Master::parse_test_actions(spec.actions.clone())?;

        //Get a set of all nodes performing test actions
        let active_nodes = Master::get_active_nodes(&actions);

        //Check if the sleep_time needs to be overriden
        if let Some(st) = sleep_time_override {
            let new_sleep_time = st.parse::<u64>().map_err(|e| {
                let err_msg = String::from("Failed to parse new worker sleep_time");
                MeshSimError {
                    kind: MeshSimErrorKind::Master(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;
            self.sleep_time_override = Some(new_sleep_time);
        }

        //Handles for all the threads that will run test actions
        let mut action_handles = Vec::new();

        //First of all, schedule the End_test action, so that the test will finish even if things go wrong
        let end_test_handle = self.testaction_end_test(self.duration, Arc::clone(&self.finish_test))?;
        action_handles.push(end_test_handle);

        //Start all workers
        let conn_str = get_connection_string_from_file(&self.env_file)?;
        for (_, val) in spec.initial_nodes.iter_mut() {
            //Assign a protocol for the worker
            val.protocol = Some(spec.protocol);

            //Start the child process
            let listen_for_commands = active_nodes.contains(&val.worker_name);

            match Master::run_worker(
                &self.worker_binary,
                &self.work_dir,
                listen_for_commands,
                &val,
                &conn_str,
                &self.logger,
            ) {
                Ok(_child_handle) => {
                    /* All good */
                },
                Err(e) => {
                    error!(&self.logger, "Error starting new worker: {}", &e);
                    if let Some(cause) = e.cause {
                        error!(&self.logger, "Cause: {}", cause);
                    }
                }
            }
        }

        info!(&self.logger, "All initial nodes started");

        //Start mobility thread
        match self.start_mobility_thread() {
            Ok(_h) => {
                info!(&self.logger, "Mobility thread started");
                //action_handles.push(h);
            }
            Err(e) => {
                error!(&self.logger, "Failed to start mobility thread: {}", e);
            }
        }

        //Add the available_nodes pool to the master.
        self.available_nodes = Arc::new(spec.available_nodes);
        //debug!("Available nodes: {:?}", &self.available_nodes);

        //Schedule all test actions.
        action_handles.append(&mut self.schedule_test_actions(actions, active_nodes)?);
        info!(
            &self.logger,
            "{} test actions scheduled",
            action_handles.len()
        );

        //All actions have been scheduled. Wait for all actions to be executed and then exit.
        for h in action_handles {
            match h.join() {
                Ok(_) => {
                    /* All good */
                },
                Err(e) => {
                    /* The thread exited with an error */
                    error!(self.logger, "Thread exited with the following error: {:?}", &e);
                }
            }
        }

        //Check if the regisration server had any errors
        match self.rs_handle.join() {
            Ok(_) => {
                /* All good */
            },
            Err(e) => {
                error!(self.logger, "RegistrationServer thread exited with the following error: {:?}", &e);
            },
        }

        // Now that all Master threads have finished, perform any cleanup required.
        // Firstly, we'll give the worker processes a little time to process the Finish command, in case they were overloaded
        // and have not yet processed it.
        let cleanup_sleep = Duration::from_nanos(CLEANUP_SLEEP);
        thread::sleep(cleanup_sleep);

        //Now check if any worker processes are lingering.
        let mut system = sysinfo::System::new_all();
        //Refresh the system to get new info about running processes
        system.refresh_all();
        let worker_path = Path::new(&self.worker_binary);
        let worker_program_name = worker_path.file_name().unwrap_or_default();
        let logger = self.logger.clone();
        system.get_processes()
            .iter()
            .filter(|(_pid, p)| p.name() == worker_program_name)
            .for_each(|(pid, p)| { 
                error!(&logger, "Process {} did not exit on time", pid; "cmd" => p.cmd().join(" "));
                if p.kill(Signal::Kill) {
                    info!(&logger, "Kill signal sent to process {}", pid);
                } else {
                    error!(&logger, "Failed to send kill signal to process {}", pid);
                }
            });

        Ok(())
    }

    fn parse_test_actions(action_strings: Vec<String>) -> Result<Vec<TestActions>, MeshSimError> {
        let mut actions = Vec::new();
        for a in action_strings {
            let action = TestActions::from_str(&a)?;
            actions.push(action);
        }
        Ok(actions)
    }

    fn get_active_nodes(actions: &[TestActions]) -> HashSet<String> {
        let mut active_nodes = HashSet::new();

        for a in actions {
            match a {
                TestActions::Ping(src, _dst, _time) => {
                    active_nodes.insert(String::from(src.as_str()));
                }
                TestActions::AddSource(src, _profile, _time) => {
                    active_nodes.insert(String::from(src.as_str()));
                }
                _ => { /* No other action requires communicating with the node during the test */ }
            }
        }

        active_nodes
    }

    fn schedule_test_actions(
        &mut self,
        actions: Vec<TestActions>,
        active_nodes: HashSet<String>,
    ) -> Result<Vec<JoinHandle<()>>, MeshSimError> {
        let mut thread_handles = Vec::new();

        for action in actions {
            let action_handle = match action {
                TestActions::EndTest(time) => self.testaction_end_test(time, Arc::clone(&self.finish_test))?,
                TestActions::AddNode(name, time) => {
                    self.testaction_add_node(name.clone(), active_nodes.contains(&name), time)?
                }
                TestActions::KillNode(name, time) => self.testaction_kill_node(name, time)?,
                TestActions::Ping(src, dst, time) => {
                    let num = self.rng.next_u64() % 1024;
                    self.testaction_ping_node(src, dst, num, time)?
                },
                TestActions::AddSource(src, profile, time) => {
                    self.testaction_add_source(src, profile, time)?
                }
            };
            thread_handles.push(action_handle);
        }
        Ok(thread_handles)
    }

    fn testaction_end_test(&self, time: u64, finish_test: Arc<AtomicBool>) -> Result<JoinHandle<()>, MeshSimError> {
        let workers_handle = Arc::clone(&self.workers);
        let logger = self.logger.clone();

        let handle = thread::spawn(move || {
            let test_endtime = Duration::from_nanos(time * ONE_MILLISECOND_NS);
            info!(logger, "End_Test action: Scheduled for {:?}", &test_endtime);
            thread::sleep(test_endtime);
            info!(logger, "End_Test action: Starting");
            let mut workers_handle = match workers_handle.lock() {
                Ok(h) => h,
                Err(e) => {
                    let msg = format!("Could not acquire lock to list of workers: {}", e);
                    error!(logger, "{}", msg);
                    return;
                }
            };
            let workers_handle = workers_handle.deref_mut();
            let mut i = 0;
            for (_id, addr) in workers_handle.values() {
                // let mut h = handle.lock().expect("Could not get lock to worker handle");
                // info!(logger, "Killing worker pid {}", h.id());
                match Master::send_command(addr, Commands::Finish, &logger) {
                    Ok(_) => {
                        info!(logger, "Process killed.");
                        i += 1;
                    }
                    Err(_) => info!(logger, "Process was not running."),
                }
            }
            //Signal the end to other threads
            finish_test.store(true, Ordering::SeqCst);
            info!(
                logger,
                "End_Test action: Finished. {} processes terminated.", i
            );
        });
        Ok(handle)
    }

    fn send_command(addr: &str, cmd: Commands, logger: &Logger) -> Result<(), MeshSimError> {
        let socket = Socket::new(Domain::unix(), Type::dgram(), None)
            .map_err(|e| {
                let err_msg = String::from("Could not create socket");
                MeshSimError {
                    kind: MeshSimErrorKind::Networking(err_msg),
                    cause: Some(Box::new(e)),
                }
        })?;
        let worker_addr = SockAddr::unix(addr)
        .map_err(|e| {
            let err_msg = format!("Could not create a UDS to {}", &addr);
            MeshSimError {
                kind: MeshSimErrorKind::Configuration(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        let data = bincode::serialize(&cmd)
            .map_err(|e| {
                let err_msg = String::from("Could not serialise command");
                MeshSimError {
                    kind: MeshSimErrorKind::Serialization(err_msg),
                    cause: Some(Box::new(e)),
                }
        })?;
        let _res = socket.send_to(&data, &worker_addr)
            .map_err(|e| {
                let err_msg = String::from("Failed to send command");
                MeshSimError {
                    kind: MeshSimErrorKind::Networking(err_msg),
                    cause: Some(Box::new(e)),
            }
        })?;
        debug!(logger," Command sent to {}", addr);

        Ok(())
    }

    fn testaction_add_node(
        &self,
        name: String,
        accept_commands: bool,
        time: u64,
    ) -> Result<JoinHandle<()>, MeshSimError> {
        let available_nodes = Arc::clone(&self.available_nodes);
        let worker_binary = self.worker_binary.clone();
        let work_dir = self.work_dir.clone();
        let logger = self.logger.clone();
        let conn_str = get_connection_string_from_file(&self.env_file)?;

        let handle = thread::spawn(move || {
            let test_endtime = Duration::from_nanos(time * ONE_MILLISECOND_NS);
            info!(
                logger,
                "Add_Node ({}) action: Scheduled for {:?}", &name, &test_endtime
            );
            thread::sleep(test_endtime);
            info!(logger, "Add_Node ({}) action: Starting", &name);

            let config = match available_nodes.get(&name) {
                Some(config) => {
                    config
                }
                None => {
                    error!(
                        logger, 
                        "Add_Node ({}) action Failed. Worker configuration not found in available_workers pool.",
                        &name
                    );
                    return 
                }
            };

            match Master::run_worker(
                &worker_binary,
                &work_dir,
                accept_commands,
                config,
                &conn_str,
                &logger,
            ) {
                Ok(_child_handle) => {
                    /* All good */
                }
                Err(e) => {
                    error!(logger, "Error running worker: {:?}", e);
                }
            }
        });
        Ok(handle)
    }

    fn testaction_kill_node(
        &self,
        name: String,
        time: u64,
    ) -> Result<JoinHandle<()>, MeshSimError> {
        let workers = Arc::clone(&self.workers);
        let logger = self.logger.clone();

        let handle = thread::spawn(move || {
            let killtime = Duration::from_nanos(time * ONE_MILLISECOND_NS);
            info!(
                logger,
                "Kill_Node ({}) action: Scheduled for {:?}", &name, &killtime
            );
            thread::sleep(killtime);
            info!(logger, "Kill_Node ({}) action: Starting", &name);

            let workers = workers.lock();
            match workers {
                Ok(mut w) => {
                    if let Some((_w_name, (_id, addr))) = w.remove_entry(&name) {
                        // let mut c = child.1.lock().expect("Could not get lock to worker handle");
                        match Master::send_command(&addr, Commands::Finish, &logger) {
                            Ok(_) => {
                                // let exit_status = c.wait();
                                info!(
                                    logger,
                                    "Kill_Node ({}) action: Worker killed.",
                                    &name,
                                );
                            }
                            Err(e) => error!(
                                logger,
                                "Kill_Node ({}) action: Failed to kill process with error {}",
                                &name,
                                e
                            ),
                        }
                    } else {
                        error!(
                            logger,
                            "Kill_Node ({}) action: Process not found in Master's collection.",
                            &name
                        );
                    }
                }
                Err(e) => {
                    error!(logger, "Kill_Node ({}) action: Could not obtain lock to workers: {}. Process not killed.", &name, e);
                }
            }
        });
        Ok(handle)
    }

    fn testaction_ping_node(
        &self,
        source: String,
        destination: String,
        num: u64,
        time: u64,
    ) -> Result<JoinHandle<()>, MeshSimError> {
        let workers = Arc::clone(&self.workers);
        let logger = self.logger.clone();

        let handle = thread::spawn(move || {
            let pingtime = Duration::from_nanos(time * ONE_MILLISECOND_NS);
            info!(
                logger,
                "Ping {}->{} action: Scheduled for {:?}", &source, &destination, &pingtime
            );
            thread::sleep(pingtime);
            info!(
                logger,
                "Ping {}->{} action: Starting", &source, &destination
            );

            let workers = match workers.lock() {
                Ok(w) => {
                    w
                }
                Err(_e) => {
                    error!(
                        logger,
                        "Ping {}->{} action: Could not obtain lock to workers, Action aborted.",
                        &source,
                        &destination
                    );
                    return;
                }
            };

            let (_id, addr) = match workers.get(&source) {
                Some(data) => { 
                    data 
                },
                None => {
                    error!(
                        logger,
                        "Ping {}->{} action: Process {} not found in active worker pool",
                        &source,
                        &destination,
                        &source
                    );
                    return;
                }
            };

            let payload = format!("PING{}", num);
            let cmd = Commands::Send(destination.clone(), payload.as_bytes().to_vec());

            match Master::send_command(addr, cmd, &logger) {
                Ok(_) => { 
                    info!(
                        logger,
                        "Ping {}->{} action: completed", &source, &destination
                    );
                },
                Err(_e) => { 
                    error!(logger, "Failed to send PING command to {}", &source);
                },
            }

        });
        Ok(handle)
    }

    fn testaction_add_source(
        &self,
        source: String,
        profile: SourceProfiles,
        time: u64,
    ) -> Result<JoinHandle<()>, MeshSimError> {
        let tb = thread::Builder::new();
        let start_time = Duration::from_nanos(time * ONE_MILLISECOND_NS);
        let workers = Arc::clone(&self.workers);
        let logger = self.logger.clone();
        let random_seed = self.random_seed;

        let handle = tb
            .name(format!("[Source]:{}", &source))
            .spawn(move || {
                unsafe {
                    let new_nice = nice(SYSTEM_THREAD_NICE);
                    debug!(logger, "[Source]:{}: New priority: {}", &source, new_nice);
                }
                info!(
                    logger,
                    "[Source]:{}: Scheduled to start in {:?}", &source, &start_time
                );
                thread::sleep(start_time);

                match profile {
                    SourceProfiles::CBR(dest, pps, size, dur) => {
                        let total_packets: f64 = (dur * pps as u64) as f64 / 1000.0;
                        info!(logger, "[Source]: Will transmit {} packets", total_packets);
                        match Master::start_cbr_source(
                            source, dest, pps, size, dur, random_seed,  workers, &logger,
                        ) {
                            Ok(_) => { /* Source finished without issues*/ }
                            Err(e) => {
                                error!(logger, "Source error:");
                                error!(logger, "{}", &e);
                                if let Some(cause) = e.cause {
                                    error!(logger, "Cause: {}", cause);
                                }
                            }
                        }
                    }
                }
            })
            .map_err(|e| {
                let err_msg = String::from("Failed to spawn source thread");
                MeshSimError {
                    kind: MeshSimErrorKind::Master(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;

        Ok(handle)
    }

    fn start_cbr_source(
        source: String,
        destination: String,
        packets_per_second: usize,
        packet_size: usize,
        duration: u64,
        random_seed: u64,
        workers: Arc<Mutex<HashMap<String, Process>>>,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        let dur = Duration::from_nanos(duration * ONE_MILLISECOND_NS);
        info!(
            logger,
            "[Source]:{}: Starting. Will run for {}.{} seconds",
            &source,
            &dur.as_secs(),
            &dur.subsec_nanos()
        );
        info!(
            logger,
            "[Source]:{}: Sending {} packets per second of size {} bytes",
            &source,
            packets_per_second,
            packet_size
        );

        let mut rng = StdRng::seed_from_u64(random_seed);
        let mut data: Vec<u8> = iter::repeat(0u8).take(packet_size).collect();
        let estimated_duration_per_packet = ONE_SECOND_NS / packets_per_second as u64; //In nanoseconds
        let mut packet_counter: u64 = 0;
        let addr = {
            let worker_list = workers.lock().expect("Could not lock workers list");
            if let Some((_id, addr)) = worker_list.get(&source) {
                addr.clone()
            } else {
                let err_msg = format!("Could not find process for {}", &source);
                let error = MeshSimError {
                    kind: MeshSimErrorKind::Contention(err_msg),
                    cause: None,
                };
                return Err(error);
            }
        };

        let trans_time = Instant::now();

        while trans_time.elapsed() < dur {
            let ts = Utc::now();
            packet_counter += 1;

            rng.fill_bytes(&mut data[..]);
            let cmd = Commands::Send(destination.clone(), data.clone());

            match Master::send_command(&addr, cmd, &logger) {
                Ok(_) => { 
                    /* All good */
                    info!(
                        logger,
                        "[Source:{}] - Send command {} written successfully",
                        source,
                        packet_counter,
                    );
                },
                Err(_e) => { 
                    error!(logger, "Could not send SEND command to worker"; "node" => &source);
                },
            }
            //Calculating pause time
            // println!("Here!");
            let elapsed = Utc::now().timestamp_nanos() - ts.timestamp_nanos();
            // println!("Ellapsed: {}ns", elapsed);
            // debug!(logger, "Ellapsed: {}ns", elapsed);

            /*
            TODO: If elapsed is larger than estimated_duration_per_packet, perhaps I should capture the 
            difference and substract it from the sleep time of the next iteration, such that the source does 
            not drift. 
            */
            let pause_time = std::cmp::max(estimated_duration_per_packet - elapsed as u64, 0);
            let pause = Duration::from_nanos(pause_time);
            debug!(
                logger,
                "[Source]:{} sleeping for {}.{} seconds",
                &source,
                &pause.as_secs(),
                &pause.subsec_nanos()
            );
            thread::sleep(pause);
        }
        info!(logger, "[Source]:{}: Finished", &source);
        Ok(())
    }

    fn start_mobility_thread(&self) -> Result<JoinHandle<()>, MeshSimError> {
        let tb = thread::Builder::new();
        let sim_start_time = Instant::now();
        let sim_end_time = Duration::from_nanos(self.duration * ONE_MILLISECOND_NS);
        let m_model = self.mobility_model.clone();
        let logger = self.logger.clone();
        // let mut paused_workers: HashMap<i32, PendingWorker> = HashMap::new();
        let _sleep_time_override = self.sleep_time_override;
        let conn = get_db_connection(&self.logger)?;
        let test_area = self.test_area;
        let random_seed = self.random_seed;
        
        tb.name(String::from("MobilityThread")).spawn(move || {

            let model = m_model.unwrap_or_else(|| { 
                info!(
                    logger,
                    "No mobility model defined. Setting Stationary model"
                );
                MobilityModels::Stationary { 
                    period: Some(DEFAULT_MOBILITY_PERIOD) 
                }
            });

            let mut mobility_handler = match Master::build_mobility_handler(model, random_seed, test_area, logger.clone()) {
                Ok(mh) => mh,
                Err(e) => {
                    //The mobility thread can't function without a handler. Exit the thread.
                    error!(logger, "{}", e);
                    return;
                },
            };

            let update_time = Duration::from_nanos(mobility_handler.get_mobility_period()); //All velocities are expresed in meters per second.
            let ratio = mobility_handler.get_mobility_period() as f64 / ONE_SECOND_NS as f64;

            while Instant::now().duration_since(sim_start_time) < sim_end_time {

                // Let one second elapse
                thread::sleep(update_time);

                //Update all worker positions
                let _res = match update_worker_positions(&conn, ratio) {
                    Ok(v) => { v },
                    Err(e) => {
                        error!(logger, "Error updating worker positions: {}", e);
                        continue;
                    }
                };

                //Pass control to mobility model
                let new_nodes_state = match mobility_handler.handle_iteration() {
                    Ok(npn) => {
                        /* All good! */
                        npn
                    },
                    Err(e) => {
                        warn!(logger, "Mobility error: {}", e);
                        continue;
                    }
                };

                for n in new_nodes_state {
                    log_node_state("A node reached its destination", n, &logger);
                }
            }

            //Before exiting the mobility loop, log the final destinations of all nodes.
            let all_worker_positions = select_all_workers_state(&conn).expect("whoops");
            for node in all_worker_positions {
                log_node_state("Final position reached", node, &logger);
                
            }

        })
        .map_err(|e| { 
            let err_msg = String::from("Failed to start mobility thread");
            MeshSimError {
                kind: MeshSimErrorKind::Worker(err_msg),
                cause: Some(Box::new(e)),
            }
        })
    }

    fn build_mobility_handler(model: MobilityModels, random_seed: u64, test_area: Area, logger: Logger) -> Result<Box<dyn MobilityHandler>, MeshSimError> {
        let conn = get_db_connection(&logger)?;

        let handler: Box<dyn MobilityHandler> = match model {
            MobilityModels::Stationary{ period } => { 
                let _ = Stationary::new(&conn, &logger, period);
                let err_msg = "Exiting thread since Stationary model is selected".to_string();
                let e = MeshSimError{
                    kind: MeshSimErrorKind::Configuration(err_msg),
                    cause: None
                };
                return Err(e);
            },
            MobilityModels::RandomWaypoint{ pause_time, period } => { 
                let h = RandomWaypoint::new(
                    HUMAN_SPEED_MEAN,
                    HUMAN_SPEED_STD_DEV,
                    random_seed,
                    test_area,
                    pause_time,
                    conn,
                    logger,
                    period
                )?;
                Box::new(h)
            },
            MobilityModels::IncreasedMobility{vel_mean: _, vel_std: _, vel_increase, pause_time, period } => { 
                let h = IncreasedMobility::new(
                    vel_increase,
                    random_seed,
                    test_area,
                    pause_time,
                    conn,
                    logger,
                    period
                );
                Box::new(h)                
            },
        };

        Ok(handler)
    }

    /// Creates a new socket for the Master/worker registration
    pub fn new_registration_socket() -> Result<Socket, MeshSimError> {
        Socket::new(Domain::ipv6(), Type::stream(), Some(Protocol::tcp())).map_err(|e| {
            let err_msg = String::from("Failed to create new socket");
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })
    }

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
