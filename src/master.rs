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

use crate::mobility2::*;
use crate::worker::worker_config::WorkerConfig;
use crate::worker::commands::Commands;
use crate::{MeshSimError, MeshSimErrorKind};
use libc::{c_int, nice};
use rand::distributions::{Normal, Uniform};
use rand::{thread_rng, Rng, RngCore};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use serde_cbor::de::*;

// use rusqlite::Connection;
use chrono::{DateTime, Utc};
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
use serde_cbor::ser::*;

//Sub-modules declaration
///Modules that defines the functionality for the test specification.
pub mod test_specification;
mod workloads;

const RANDOM_WAYPOINT_WAIT_TIME: u64 = 1000;
const SYSTEM_THREAD_NICE: c_int = -20; //Threads that need to run with a higher priority will use this
/// Port on which the Master listens for workers to register
pub const MASTER_LISTEN_PORT: u16 = 9999;

type PendingWorker = (DateTime<Utc>, Velocity);

///Different supported mobility models
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
#[serde(tag = "Model")]
pub enum MobilityModels {
    /// Random waypoint model
    RandomWaypoint {
        /// The amount of time a node will wait once it reaches its destination.
        pause_time: u64,
    },
    /// No movement
    Stationary,
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
                let pause_time = parts[1].parse::<u64>().unwrap_or(RANDOM_WAYPOINT_WAIT_TIME);
                Ok(MobilityModels::RandomWaypoint { pause_time })
            }
            "STATIONARY" => Ok(MobilityModels::Stationary),
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
    fn from(err: toml::de::Error) -> MasterError {
        MasterError::TOML(err)
    }
}

impl From<serde_cbor::Error> for MasterError {
    fn from(err: serde_cbor::Error) -> MasterError {
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

impl error::Error for MasterError {
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

    fn cause(&self) -> Option<&dyn error::Error> {
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
    pub fn new(work_dir: String, log_file: String, db_name: String, logger: Logger) -> Result<Master, MeshSimError> {
        let workers = Arc::new(Mutex::new(HashMap::new()));
        let wb = String::from("./worker_cli");
        let an = HashMap::new();
        let env_file = create_db_objects(&work_dir, &db_name, &logger)?;

        debug!(&logger, "Using connection file: {}", &env_file);
        debug!(&logger, "Using DB name: {}", &db_name);

        // Master::start_registration_server(port)?;
        let handle = Master::start_registration_server(
            MASTER_LISTEN_PORT,
            &env_file,
            Arc::clone(&workers),
            &logger,
        )?;

        let m = Master {
            workers,
            work_dir,
            worker_binary: wb,
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
        };


        Ok(m)
    }

    fn start_registration_server(
        listening_port: u16,
        env_file: &String,
        workers: Arc<Mutex<HashMap<String, (i32, String)>>>,
        logger: &Logger,
    ) -> Result<JoinHandle<()>, MeshSimError> {
        let sock = Master::new_socket()?;
        let base_addr = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
                listening_port
        );
        sock.bind(&SockAddr::from(base_addr)).map_err(|e| {
            let err_msg = format!("Could not bind socket to address {}", &base_addr);
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        let conn = get_db_connection_by_file(&env_file, logger)?;
        let log = logger.clone();
        let workers = Arc::clone(&workers);

        let j = thread::Builder::new()
        .name(String::from("RegistrationServer"))
        .spawn(move || {
            //64kb buffer
            let mut buffer = [0; 65536];
            loop {
                let (bytes_read, _peer_address) = match sock.recv_from(&mut buffer) {
                    Ok(data) => {
                        data
                    },
                    Err(e) => {
                        //No message read
                        error!(log, "Error reading from RegistrationServer socket: {}", &e);
                        continue;
                    }
                };

                if bytes_read == 0 {
                    /* 0 bytes read*/
                    warn!(log, "Successful read from socket was empty (0 bytes)");
                    continue;
                }

                let data = buffer[..bytes_read].to_vec();
                let cmd: Commands = from_slice(&data).expect("Could not deserialise message from worker");

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

                        let id = match register_worker(
                            &conn,
                            w_name.clone(),
                            w_id,
                            pos,
                            vel.unwrap_or_default(),
                            &dest,
                            sr_address,
                            lr_address,
                            &log,
                        ) {
                            Ok(id) => {
                                id
                            },
                            Err(e) => {
                                error!(log, "Error registering new worker: {}", e);
                                continue;
                            }
                        };

                        //Save the db_id of the worker as well as the handle to its process.
                        let mut workers = workers
                            .lock()
                            .expect("Could not lock workers list");
                        workers.insert(w_name, (id, cmd_address));
                    },
                    _ => {
                        /* The master does  not support any other commands*/
                        warn!(log, "Unsupported command received {:?}", cmd);
                    }
                }
            };
        })
        .map_err(|e| {
            let err_msg = String::from("Failed to spawn RegistrationServer thread");
            MeshSimError {
                kind: MeshSimErrorKind::Master(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

        info!(&logger, "Master has been initialised");
        Ok(j)
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
        &mut self,
        mut spec: test_specification::TestSpec,
        sleep_time_override: Option<&str>,
        logger: &Logger,
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
        let end_test_handle = self.testaction_end_test(self.duration)?;
        action_handles.push(end_test_handle);

        // Obtain database connection
        let conn = get_db_connection_by_file(&self.env_file, &self.logger)?;

        //Start all workers
        let conn_str = get_connection_string_from_file(&self.env_file)?;
        for (_, val) in spec.initial_nodes.iter_mut() {
            //Assign a protocol for the worker
            val.protocol = Some(spec.protocol.clone());

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

        //let cl = self.start_command_loop_thread()?;

        //All actions have been scheduled. Wait for all actions to be executed and then exit.
        for h in action_handles {
            match h.join() {
                Ok(_) => {}
                Err(_) => {
                    warn!(self.logger, "Couldn't join on thread");
                }
            }
        }

        //cl.join();

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
        &self,
        actions: Vec<TestActions>,
        active_nodes: HashSet<String>,
    ) -> Result<Vec<JoinHandle<()>>, MeshSimError> {
        let mut thread_handles = Vec::new();

        for action in actions {
            let action_handle = match action {
                TestActions::EndTest(time) => self.testaction_end_test(time)?,
                TestActions::AddNode(name, time) => {
                    self.testaction_add_node(name.clone(), active_nodes.contains(&name), time)?
                }
                TestActions::KillNode(name, time) => self.testaction_kill_node(name, time)?,
                TestActions::Ping(src, dst, time) => self.testaction_ping_node(src, dst, time)?,
                TestActions::AddSource(src, profile, time) => {
                    self.testaction_add_source(src, profile, time)?
                }
            };
            thread_handles.push(action_handle);
        }
        Ok(thread_handles)
    }

    fn testaction_end_test(&self, time: u64) -> Result<JoinHandle<()>, MeshSimError> {
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
            info!(
                logger,
                "End_Test action: Finished. {} processes terminated.", i
            );
        });
        Ok(handle)
    }

    fn send_command(addr: &String, cmd: Commands, logger: &Logger) -> Result<(), MeshSimError> {
        let socket = Socket::new(Domain::unix(), Type::dgram(), None)
            .map_err(|e| {
                let err_msg = String::from("Could not create socket");
                MeshSimError {
                    kind: MeshSimErrorKind::Networking(err_msg),
                    cause: Some(Box::new(e)),
                }
        })?;
        let worker_addr = SockAddr::unix(addr).unwrap();
        let data = to_vec(&cmd)
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
        info!(logger," Command sent: {:?}", &cmd);

        Ok(())
    }

    fn testaction_add_node(
        &self,
        name: String,
        accept_commands: bool,
        time: u64,
    ) -> Result<JoinHandle<()>, MeshSimError> {
        let available_nodes = Arc::clone(&self.available_nodes);
        let workers = Arc::clone(&self.workers);
        let worker_binary = self.worker_binary.clone();
        let work_dir = self.work_dir.clone();
        let logger = self.logger.clone();
        let conn_str = get_connection_string_from_file(&self.env_file)?;
        let conn = get_db_connection_by_file(&self.env_file, &self.logger)?;

        let handle = thread::spawn(move || {
            let test_endtime = Duration::from_millis(time);
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

            let worker_id = match config.worker_id {
                Some(ref id) => id.clone(),
                None => WorkerConfig::gen_id(config.random_seed),
            };

            match Master::run_worker(
                &worker_binary,
                &work_dir,
                accept_commands,
                config,
                &conn_str,
                &logger,
            ) {
                Ok(mut child_handle) => {
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
            let killtime = Duration::from_millis(time);
            info!(
                logger,
                "Kill_Node ({}) action: Scheduled for {:?}", &name, &killtime
            );
            thread::sleep(killtime);
            info!(logger, "Kill_Node ({}) action: Starting", &name);

            let workers = workers.lock();
            match workers {
                Ok(mut w) => {
                    if let Some((_id, addr)) = w.get(&name) {
                        // let mut c = child.1.lock().expect("Could not get lock to worker handle");
                        match Master::send_command(addr, Commands::Finish, &logger) {
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
        time: u64,
    ) -> Result<JoinHandle<()>, MeshSimError> {
        let workers = Arc::clone(&self.workers);
        let logger = self.logger.clone();

        let handle = thread::spawn(move || {
            let pingtime = Duration::from_millis(time);
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

            let mut rng = thread_rng();
            let r: u64 = rng.next_u64() % 1024;
            let payload = format!("PING{}", r);
            let cmd = Commands::Send(destination.clone(), payload.as_bytes().to_vec());

            match Master::send_command(addr, cmd, &logger) {
                Ok(_) => { 
                    info!(
                        logger,
                        "Ping {}->{} action: completed", &source, &destination
                    );
                },
                Err(e) => { 
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
        let start_time = Duration::from_millis(time);
        let workers = Arc::clone(&self.workers);
        let logger = self.logger.clone();

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
                            source, dest, pps, size, dur, workers, &logger,
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
        workers: Arc<Mutex<HashMap<String, Process>>>,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        let dur = Duration::from_millis(duration);
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

        let mut rng = thread_rng();
        let mut data: Vec<u8> = iter::repeat(0u8).take(packet_size).collect();
        let iter_threshold = 1_000_000_000u32 / packets_per_second as u32; //In nanoseconds
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
        let mut iteration = Instant::now();

        while trans_time.elapsed() < dur {
            packet_counter += 1;

            rng.fill_bytes(&mut data[..]);
            let encoded_data = base64::encode(&data);
            let payload = format!("SEND {} {}\n", &destination, &encoded_data);
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
                Err(e) => { 
                    error!(logger, "Could not send SEND command to worker"; "node" => &source);
                },
            }
            //Calculating pause time
            let iter_duration = iteration.elapsed().subsec_nanos();
            let pause_time = std::cmp::max(
                iter_threshold
                    - (std::cmp::max(iter_duration as i32 - iter_threshold as i32, 0i32)) as u32,
                0u32,
            );
            iteration = Instant::now();
            let pause = Duration::from_nanos(u64::from(pause_time));
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
        let update_time = Duration::from_millis(1000); //All velocities are expresed in meters per second.
        let sim_start_time = Instant::now();
        let sim_end_time = Duration::from_millis(self.duration);
        let width = self.test_area.width;
        let height = self.test_area.height;
        let duration = self.duration;
        let mut initialized: bool = Default::default();
        let m_model = self.mobility_model.clone();
        let logger = self.logger.clone();
        let mut paused_workers: HashMap<i32, PendingWorker> = HashMap::new();
        let sleep_time_override = self.sleep_time_override.clone();
        let conn = get_db_connection_by_file(&self.env_file, &self.logger)?;

        tb.name(String::from("MobilityThread")).spawn(move || {
            let mut rng = thread_rng();
            let width_sample = Uniform::new(0.0, width);
            let height_sample = Uniform::new(0.0, height);
            let walking_sample = Normal::new(HUMAN_SPEED_MEAN, HUMAN_SPEED_STD_DEV);

            let model = match m_model {
                Some(m) => m,
                None => {
                    info!(
                        logger,
                        "No mobility model defined. Setting Stationary model"
                    );
                    MobilityModels::Stationary
                }
            };

            let sleep_time = match sleep_time_override {
                Some(time) => time,
                None => match &model {
                    MobilityModels::RandomWaypoint { pause_time } => *pause_time,
                    MobilityModels::Stationary => duration,
                },
            };

            while Instant::now().duration_since(sim_start_time) < sim_end_time {
                //Restart movement for workers whose pause has ended.
                let mut restarted_workers: Vec<i32> = Vec::new();
                for (key, val) in paused_workers.iter() {
                    let worker_id = key;
                    let restart_time = &val.0;
                    let velocity = &val.1;
                    if *restart_time <= Utc::now() {
                        match update_worker_vel(&conn, *velocity, *worker_id, &logger) {
                            Ok(_r) => restarted_workers.push(*worker_id),
                            Err(e) => {
                                error!(
                                    logger,
                                    "Failed updating target for worker_id {}: {}", worker_id, &e
                                );
                                if let Some(cause) = e.cause {
                                    error!(logger, "Cause: {}", cause);
                                }
                            }
                        }
                    }
                }

                //Remove the workers that were succesfully updated from the pending list
                paused_workers.retain(|&k, _| !restarted_workers.contains(&k));

                //Wait times will be uniformly sampled from the remain simulation time.
                let mut lower_bound: u64 = sim_start_time.elapsed().as_secs() * 1000;
                lower_bound += u64::from(sim_start_time.elapsed().subsec_millis());
                let wait_sample = Uniform::new(lower_bound, duration);

                thread::sleep(update_time);

                // ****** DEBUG ******* //
                // let workers = get_all_worker_positions(&conn)?;
                // for w in workers {
                //     debug!(logger, "{}({}): ({}, {})", w.1, w.0, w.2, w.3);
                // }

                match model {
                    MobilityModels::RandomWaypoint { pause_time: _ } => {
                        Master::handle_random_waypoint(
                            &conn,
                            &width_sample,
                            &height_sample,
                            &walking_sample,
                            &wait_sample,
                            sleep_time,
                            &mut paused_workers,
                            &mut rng,
                            &logger,
                        );
                    }
                    MobilityModels::Stationary => {
                        if !initialized {
                            Master::handle_stationary(&conn, &logger);
                            initialized = true;
                        }
                    }
                }

                //Update worker positions
                match update_worker_positions(&conn) {
                    Ok(rows) => {
                        debug!(logger, "The position of {} workers has been updated", rows);
                    }
                    Err(e) => {
                        error!(logger, "Error updating worker positions: {}", e);
                    }
                }
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

    fn handle_stationary(conn: &PgConnection, logger: &Logger) {
        let _rows = match stop_all_workers(conn) {
            Ok(rows) => rows,
            Err(e) => {
                error!(logger, "{}", e);
                0
            }
        };
    }

    fn handle_random_waypoint(
        conn: &PgConnection,
        width_sample: &Uniform<f64>,
        height_sample: &Uniform<f64>,
        walking_sample: &Normal,
        _wait_sample: &Uniform<u64>,
        pause_time: u64,
        paused_workers: &mut HashMap<i32, PendingWorker>,
        rng: &mut dyn RngCore,
        // db_path: &str,
        logger: &Logger,
    ) {
        debug!(logger, "Entered function handle_random_waypoint");

        //Select workers that reached their destination
        let rows = match select_workers_that_arrived(&conn) {
            Ok(rows) => rows,
            Err(e) => {
                error!(logger, "Error updating worker positions: {}", e);
                HashMap::new()
            }
        };

        if !rows.is_empty() {
            info!(
                logger,
                "{} workers have reached their destinations",
                rows.len()
            );
            //Stop workers that have reached their destination
            let stoppable: Vec<i32> = rows.keys().copied().collect();
            match stop_workers(&conn, &stoppable, logger) {
                Ok(r) => {
                    info!(logger, "{} workers have been stopped", r);
                }
                Err(e) => {
                    error!(logger, "Could not stop workers: {}", e);
                }
            }

            for w in rows {
                let w_id = w.0;
                let pos = w.1;

                //Calculate parameters
                let next_x: f64 = rng.sample(width_sample);
                let next_y: f64 = rng.sample(height_sample);
                let vel: f64 = rng.sample(walking_sample);
                let distance: f64 = euclidean_distance(pos.x, pos.y, next_x, next_y);
                let time: f64 = distance / vel;
                let x_vel = (next_x - pos.x) / time;
                let y_vel = (next_y - pos.y) / time;

                //Update worker target position
                let _res = update_worker_target(
                    &conn,
                    Position {
                        x: next_x,
                        y: next_y,
                    },
                    w_id,
                    &logger,
                );

                let restart_time = Utc::now() + chrono::Duration::milliseconds(pause_time as i64);
                paused_workers.insert(w_id, (restart_time, Velocity { x: x_vel, y: y_vel }));
            }
        }
    }

    fn new_socket() -> Result<Socket, MeshSimError> {
        Socket::new(Domain::ipv6(), Type::dgram(), Some(Protocol::udp())).map_err(|e| {
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
