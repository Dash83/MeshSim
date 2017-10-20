extern crate mesh_simulator;
extern crate clap;
extern crate rustc_serialize;
extern crate serde;
extern crate serde_cbor;
#[macro_use]
extern crate slog;
extern crate slog_stream;
extern crate slog_term;
extern crate slog_json;
extern crate slog_stdlog;
#[macro_use]
extern crate log;

use mesh_simulator::worker::{WorkerConfig};
use mesh_simulator::master::*;
use mesh_simulator::master;
use clap::{Arg, App, ArgMatches};
use std::str::FromStr;
use std::thread;
use slog::DrainExt;
use std::fs::{OpenOptions};
use std::path::Path;
use std::io;
use std::error;
use std::fmt;
use std::env;

const ARG_CONFIG : &'static str = "config";
const ARG_TEST : &'static str = "test";
const ARG_WORK_DIR : &'static str = "work_dir";

const ERROR_LOG_INITIALIZATION : i32 = 1;
const ERROR_EXECUTION_FAILURE : i32 = 2;

#[derive(Debug)]
enum CLIError {
    SetLogger(log::SetLoggerError),
    IO(io::Error),
    Master(master::MasterError),
    TestParsing(String),
}

enum MeshTests {
    BasicTest,
    SixNodeTest,
}

impl FromStr for MeshTests {
    type Err = CLIError;

    fn from_str(s : &str) -> Result<MeshTests, CLIError> {
        match s {
            "BasicTest" => Ok(MeshTests::BasicTest),
            "SixNodeTest" => Ok(MeshTests::SixNodeTest),
            _ => Err(CLIError::TestParsing("Unsupported test".to_string())),
        }
    }
}

impl error::Error for CLIError {
    fn description(&self) -> &str {
        match *self {
            CLIError::SetLogger(ref err) => err.description(),
            CLIError::IO(ref err) => err.description(),
            CLIError::Master(ref err) => err.description(),
            CLIError::TestParsing(ref err_str) => err_str,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            CLIError::SetLogger(ref err) => Some(err),
            CLIError::IO(ref err) => Some(err),
            CLIError::Master(ref err) => Some(err),
            CLIError::TestParsing(_) => None,
        }
    }
}

impl fmt::Display for CLIError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CLIError::SetLogger(ref err) => write!(f, "SetLogger error: {}", err),
            CLIError::IO(ref err) => write!(f, "IO error: {}", err),
            CLIError::Master(ref err) => write!(f, "Error in Master layer: {}", err),
            CLIError::TestParsing(ref err_str) => write!(f, "Error parsing test: {}", err_str),
        }
    }

}

impl From<io::Error> for CLIError {
    fn from(err : io::Error) -> CLIError {
        CLIError::IO(err)
    }
}

impl From<log::SetLoggerError> for CLIError {
    fn from(err : log::SetLoggerError) -> CLIError {
        CLIError::SetLogger(err)
    }
}

impl From<master::MasterError> for CLIError {
    fn from(err : master::MasterError) -> CLIError {
        CLIError::Master(err)
    }
}


fn init_logger(work_dir : &Path) -> Result<(), CLIError> {
    let mut log_dir_buf = work_dir.to_path_buf();
    log_dir_buf.push("log");
    
    if !log_dir_buf.exists() {
        try!(std::fs::create_dir(log_dir_buf.as_path()));
    }

    log_dir_buf.push("MeshMaster.log");
    //At this point the log folder has been created, so the path should be valid.
    let log_file_name = log_dir_buf.to_str().unwrap();

    let log_file = try!(OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(log_file_name));
    
    let console_drain = slog_term::streamer().build();
    let file_drain = slog_stream::stream(log_file, slog_json::default());
    let logger = slog::Logger::root(slog::duplicate(console_drain, file_drain).fuse(), o!());
    try!(slog_stdlog::set_logger(logger));

    Ok(())
} 

fn test_basic_test() -> Result<(), CLIError> {
    info!("Running BasicTest");
    let mut master = Master::new();
    let mut cfg1 = WorkerConfig::new();
    cfg1.worker_name = "Worker1".to_string();
    cfg1.work_dir = String::from("/tmp");

    let mut cfg2 = WorkerConfig::new();
    cfg2.worker_name = "Worker2".to_string();
    cfg2.work_dir = String::from("/tmp");

    try!(master.add_worker(cfg1));
    //Super fucking hacky. It seems the order for process start is not that deterministic.
    //TODO: Find a way to address this.
    thread::sleep(std::time::Duration::from_millis(4000)); 
    try!(master.add_worker(cfg2));

    match master.wait_for_workers() {
        Ok(_) => info!("Finished successfully."),
        Err(e) => error!("Master failed to wait for children processes with error {}", e),
    }
    
    Ok(())
}

/// This test creates 
fn test_six_node_test() -> Result<(), CLIError> {
    info!("Running SixNodeTest");
    let mut master = Master::new();
    let mut cfg1 = WorkerConfig::new();
    cfg1.worker_name = "Worker1".to_string();
    cfg1.work_dir = String::from("/tmp");

    let mut cfg2 = WorkerConfig::new();
    cfg2.worker_name = "Worker2".to_string();
    cfg2.work_dir = String::from("/tmp");

    let mut cfg3 = WorkerConfig::new();
    cfg3.worker_name = "Worker3".to_string();
    cfg3.work_dir = String::from("/tmp");

    let mut cfg4 = WorkerConfig::new();
    cfg4.worker_name = "Worker4".to_string();
    cfg4.work_dir = String::from("/tmp");

    let mut cfg5 = WorkerConfig::new();
    cfg5.worker_name = "Worker5".to_string();
    cfg5.work_dir = String::from("/tmp");

    let mut cfg6 = WorkerConfig::new();
    cfg6.worker_name = "Worker6".to_string();
    cfg6.work_dir = String::from("/tmp");
    
    try!(master.add_worker(cfg1));

    //Super fucking hacky. It seems the order for process start is not that deterministic.
    //TODO: Find a way to address this.
    thread::sleep(std::time::Duration::from_millis(4000)); 
    try!(master.add_worker(cfg2));

    thread::sleep(std::time::Duration::from_millis(4000)); 
    try!(master.add_worker(cfg3));

    thread::sleep(std::time::Duration::from_millis(4000)); 
    try!(master.add_worker(cfg4));

    thread::sleep(std::time::Duration::from_millis(4000)); 
    try!(master.add_worker(cfg5));

    thread::sleep(std::time::Duration::from_millis(4000)); 
    try!(master.add_worker(cfg6));

    match master.wait_for_workers() {
        Ok(_) => info!("Finished successfully."),
        Err(e) => error!("Master failed to wait for children processes with error {}", e),
    }
    /*
    info!("Running BasicTest");
    let mut master = Master::new();
    let mut w1 = Worker::new();
    w1.me.name = "Worker1".to_string();
    
    w1.radios[0].add_bcast_group(String::from("Group1"));
    File::create("//tmp/Group1/Worker1")?;

    let mut w2 = Worker::new();
    w2.me.name = "Worker2".to_string();
    w2.add_peers(vec![w1.me.clone()]);
    w2.radios[0].add_bcast_group(String::from("Group1"));
    w2.radios[0].add_bcast_group(String::from("Group2"));
    File::create("//tmp/Group1/Worker2")?;
    File::create("//tmp/Group2/Worker2")?;

    let mut w3 = Worker::new();
    w3.me.name = "Worker3".to_string();
    w3.add_peers(vec![w2.me.clone()]);
    w3.radios[0].add_bcast_group(String::from("Group2"));
    w3.radios[0].add_bcast_group(String::from("Group3"));
    File::create("//tmp/Group2/Worker3")?;
    File::create("//tmp/Group3/Worker3")?;

    let mut w4 = Worker::new();
    w4.me.name = "Worker4".to_string();
    w4.add_peers(vec![w3.me.clone()]);
    w4.radios[0].add_bcast_group(String::from("Group3"));
    w4.radios[0].add_bcast_group(String::from("Group4"));
    File::create("//tmp/Group3/Worker4")?;
    File::create("//tmp/Group4/Worker4")?;

    let mut w5 = Worker::new();
    w5.me.name = "Worker5".to_string();
    w5.add_peers(vec![w4.me.clone()]);
    w5.radios[0].add_bcast_group(String::from("Group4"));
    w5.radios[0].add_bcast_group(String::from("Group5"));
    File::create("//tmp/Group4/Worker5")?;
    File::create("//tmp/Group5/Worker5")?;

    let mut w6 = Worker::new();
    w6.me.name = "Worker6".to_string();
    w6.add_peers(vec![w5.me.clone()]);
    w6.radios[0].add_bcast_group(String::from("Group5"));
    File::create("//tmp/Group5/Worker6")?;

    try!(master.add_worker(w1));
    //Super fucking hacky. It seems the order for process start is not that deterministic.
    //TODO: Find a way to address this.
    thread::sleep(std::time::Duration::from_millis(2000)); 
    try!(master.add_worker(w2));

    thread::sleep(std::time::Duration::from_millis(2000)); 
    try!(master.add_worker(w3));

    thread::sleep(std::time::Duration::from_millis(2000)); 
    try!(master.add_worker(w4));

    thread::sleep(std::time::Duration::from_millis(2000)); 
    try!(master.add_worker(w5));

    thread::sleep(std::time::Duration::from_millis(2000)); 
    try!(master.add_worker(w6));

    match master.wait_for_workers() {
        Ok(_) => info!("Finished successfully."),
        Err(e) => error!("Master failed to wait for children processes with error {}", e),
    }
    */
    Ok(())
}

fn run(matches : ArgMatches) -> Result<(), CLIError> {
    let test_str = matches.value_of(ARG_TEST);

    match test_str {
        Some(data) => { 
            let selected_test = try!(data.parse::<MeshTests>());
            match selected_test {
                MeshTests::BasicTest => try!(test_basic_test()),
                MeshTests::SixNodeTest => try!(test_six_node_test())
            }
        },
        None => { 
        },
    }
    Ok(())
}

fn init(matches : &ArgMatches) -> Result<(), CLIError> {
    //Read the configuration file.
    // let mut current_dir = try!(env::current_dir());
    // let config_file_path = matches.value_of(ARG_CONFIG).unwrap_or_else(|| {
    //     //No configuration file was passed. Look for default option: current_dir + default name.
    //      current_dir.push(CONFIG_FILE_NAME);
    //      current_dir.to_str().expect("No configuration file was provided and current directory is not readable.")
    // });
    // let mut configuration = try!(load_conf_file(config_file_path));
    
    //Initialize logger
    let work_dir = try!(env::current_dir());
    try!(init_logger(work_dir.as_path()));

    //Validate the current configuration
    //try!(validate_config(&mut configuration, &matches));

    Ok(())
}

fn get_cli_parameters<'a>() -> ArgMatches<'a> {
    App::new("Master_cli")
            .version("0.1")
            .author("Marco Caballero <marco.caballero@cl.cam.ac.uk>")
            .about("CLI interface to the Master object from the mesh simulator system")
            .arg(Arg::with_name(ARG_CONFIG)
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("Sets a custom configuration file for the worker.")
                .takes_value(true))
            .arg(Arg::with_name(ARG_TEST)
                .short("test")
                .value_name("TEST_NAME")
                .help("Name of the test to be run.")
                .takes_value(true))
            .arg(Arg::with_name(ARG_WORK_DIR)
                .short("dir")
                .value_name("work_dir")
                .help("Operating directory for the program, where results and logs will be placed.")
                .takes_value(true))
            .get_matches()
}

fn main() {
    //Build CLI interface
    let matches = get_cli_parameters();

    if let Err(ref e) = init(&matches) {
        //Since we failed before initializing the logger, all we can do is log to stdout and exit with a different error code.
        println!("master_cli failed with the following error: {}", e);
        ::std::process::exit(ERROR_LOG_INITIALIZATION);
    }

    if let Err(ref e) = run(matches) {
        error!("master_cli failed with the following error: {}", e);
        ::std::process::exit(ERROR_EXECUTION_FAILURE);
    }
}