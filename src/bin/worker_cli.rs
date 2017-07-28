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
extern crate toml;
#[macro_use]
extern crate serde_derive;
extern crate rand;

use mesh_simulator::worker::{Worker};
use mesh_simulator::worker;
use clap::{Arg, App, ArgMatches};
use rustc_serialize::hex::*;
use serde_cbor::de::*;
use slog::DrainExt;
use std::fs::{OpenOptions, File};
use std::io::{self, Write, Read};
use std::fmt;
use std::error;
use std::env;
use rand::Rng;

    

const ARG_CONFIG : &'static str = "config";
const ARG_WORKER_OBJ : &'static str = "worker_obj";
const ARG_WORKER_NAME : &'static str = "worker_name";
const ARG_WORK_DIR : &'static str = "work_dir";
const ARG_RANDOM_SEED : &'static str = "random_seed";
const ARG_OPERATION_MODE : &'static str = "operation_mode";
const ARG_BROADCAST_GROUP : &'static str = "broadcast_group";
const ARG_NETWORK_RELIABILITY : &'static str = "reliability";
const ARG_NETWORK_DELAY : &'static str = "delay";
const ARG_SCAN_INTERVAL : &'static str = "scan_interval";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");
/*
const CONFIG_FILE_TEMPLATE  : &'static str = r#"
        worker_name = "{}"
        random_seed = {}
        work_dir = "{}""
        operation_mode = "{}"
        reliability = {}
        delay = {}
        scan_interval = {}
        broadcast_group = "{}"
    "#;
*/
const CONFIG_FILE_NAME  : &'static str = "worker.toml";
const ERROR_EXECUTION_FAILURE : i32 = 1;
const ERROR_LOG_INITIALIZATION : i32 = 2;


// *****************************************
// ************ Module Errors **************
// *****************************************

#[derive(Debug)]
enum CLIError {
    SetLogger(log::SetLoggerError),
    IO(io::Error),
    Worker(worker::WorkerError),
    Serialization(serde_cbor::Error),
    TOML(toml::de::Error),
}

impl error::Error for CLIError {
    fn description(&self) -> &str {
        match *self {
            CLIError::SetLogger(ref err) => err.description(),
            CLIError::IO(ref err) => err.description(),
            CLIError::Worker(ref err) => err.description(),
            CLIError::Serialization(ref err) => err.description(),
            CLIError::TOML(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            CLIError::SetLogger(ref err) => Some(err),
            CLIError::IO(ref err) => Some(err),
            CLIError::Worker(ref err) => Some(err),
            CLIError::Serialization(ref err) => Some(err),
            CLIError::TOML(ref err) => Some(err),
        }
    }
}

impl fmt::Display for CLIError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            CLIError::SetLogger(ref err) => write!(f, "SetLogger error: {}", err),
            CLIError::IO(ref err) => write!(f, "IO error: {}", err),
            CLIError::Worker(ref err) => write!(f, "Worker layer error: {}", err),
            CLIError::Serialization(ref err) => write!(f, "Serialization error: {}", err),
            CLIError::TOML(ref err) => write!(f, "TOML error: {}", err),
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

impl From<serde_cbor::Error> for CLIError {
    fn from(err : serde_cbor::Error) -> CLIError {
        CLIError::Serialization(err)
    }
}

impl From<worker::WorkerError> for CLIError {
    fn from(err : worker::WorkerError) -> CLIError {
        CLIError::Worker(err)
    }
}

impl From<toml::de::Error> for CLIError {
    fn from(err : toml::de::Error) -> CLIError {
        CLIError::TOML(err)
    }
}
// ***************End Errors****************

// *****************************************
// ************* Data types ****************
// *****************************************
#[derive(Debug, Deserialize, PartialEq)]
struct WorkerConfig {
    worker_name : String,
    work_dir : String,
    random_seed : u32,
    operation_mode : worker::OperationMode,
    broadcast_group : Option<String>,
    reliability : Option<f64>,
    delay : Option<u32>,
    scan_interval : Option<u32>,
}

impl WorkerConfig {
    pub fn new() -> WorkerConfig {
        WorkerConfig{worker_name : "worker1".to_string(),
                     work_dir : ".".to_string(),
                     random_seed : 12345, 
                     operation_mode : worker::OperationMode::Simulated,
                     broadcast_group : Some("group1".to_string()),
                     reliability : Some(1.0),
                     delay : Some(0),
                     scan_interval : Some(2000)
                    }
    }
}

// ************ End Data types *************

fn decode_worker_data<'a>(arg : &'a str) -> Result<Worker, serde_cbor::Error> {
    let e : Vec<u8> = arg.from_hex().unwrap();
    let obj : Result<Worker, _> = from_reader(&e[..]);
    obj
}

fn init_logger(matches : &ArgMatches) -> Result<(), CLIError>  { 
    let working_dir_arg = matches.value_of(ARG_WORK_DIR);
    let proc_name = matches.value_of(ARG_WORKER_NAME).unwrap_or("MeshWorker").to_string();

    let working_dir = match working_dir_arg {
        Some(dir) => dir.to_string(),
        //TODO: If no working dir parameter is passed, read from config file before defaulting to "."
        None => ".".to_string(),
    };
    let log_dir = working_dir + "/log";
    let log_file_name = log_dir + "/" + &proc_name + ".log";

    let log_file = try!(OpenOptions::new()
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(log_file_name));
    
    let console_drain = slog_term::streamer().build();
    let file_drain = slog_stream::stream(log_file, slog_json::default());
    let logger = slog::Logger::root(slog::duplicate(console_drain, file_drain).fuse(), o!("Process" => proc_name));
    try!(slog_stdlog::set_logger(logger));
    Ok(())
} 

fn run(matches : ArgMatches) -> Result<(), CLIError> {
    //Obtain individual arguments
    let worker_data = matches.value_of(ARG_WORKER_OBJ);
    let proc_name = matches.value_of(ARG_WORKER_NAME).unwrap_or("MeshWorker");
    
    let mut obj = match worker_data {
        Some(arg) => {
                try!(decode_worker_data(arg))
            },
        None => { 
            //Build worker object and set optional parameters
            info!("Building default worker.");
            Worker::new(proc_name.to_string())
        }, 

    };

   try!(obj.start());
   Ok(())
}

fn load_conf_file<'a>(file_path : &'a str) -> Result<WorkerConfig, CLIError> {
    //Check that configuration file passed exists.
    //If it doesn't exist, error out
    let mut file = try!(File::open(file_path));
    let mut file_content = String::new();
    let read_chars =  try!(file.read_to_string(&mut file_content));
    let configuration : WorkerConfig = try!(toml::from_str(&file_content));
    Ok(configuration)
    //WorkerConfig::new()
}

fn create_default_conf_file() -> Result<String, CLIError> {
    //Create configuration values
    let mut rng = try!(rand::os::OsRng::new());
    let worker_name = format!("worker{}", rng.gen::<u8>());
    let random_seed : u32 = rng.gen();
    let cur_dir = try!(env::current_dir());
    let work_dir = cur_dir.to_str().expect("Invalid directory path.");
    let operation_mode = worker::OperationMode::Simulated;
    let broadcast_group = "bc_group1";
    let reliability = 1.0;
    let delay = 0;
    let scan_interval = 2000;

    //Create configuration file
    let file_path = format!("{}{}{}", work_dir, std::path::MAIN_SEPARATOR, CONFIG_FILE_NAME);
    let mut file = try!(File::create(&file_path));

    //Write content to file
    //file.write(sample_toml_str.as_bytes()).expect("Error writing to toml file.");
    write!(file, "worker_name = \"{}\"\n", worker_name)?;
    write!(file, "random_seed = {}\n", random_seed)?;
    write!(file, "work_dir = \"{}\"\n", work_dir)?;
    write!(file, "operation_mode = \"{}\"\n", operation_mode)?;
    write!(file, "reliability = {}\n", reliability)?;
    write!(file, "delay = {}\n", delay)?;
    write!(file, "scan_interval = {}\n", scan_interval)?;
    write!(file, "broadcast_group = \"{}\"\n", broadcast_group)?;

    //mock_file.flush().expect("Error flusing toml file to disk.");
    Ok(file_path.to_string())
}

fn get_cli_parameters<'a>() -> ArgMatches<'a> {
    App::new("Worker_cli").version(VERSION)
                          .author("Marco Caballero <marco.caballero@cl.cam.ac.uk>")
                          .about("CLI interface to the worker object from the mesh simulator system")
                          .arg(Arg::with_name(ARG_CONFIG)
                                .short("c")
                                .long("config")
                                .value_name("FILE")
                                .help("Sets a custom configuration file for the worker.")
                                .takes_value(true))
                          .arg(Arg::with_name(ARG_WORKER_OBJ)
                                .short("worker")
                                .value_name("WORKER_DATA")
                                .long("Worker_Object")
                                .help("Instance of a worker object. Passed by a master process when coordinating several worker instances.")
                                .takes_value(true))
                          .arg(Arg::with_name(ARG_WORKER_NAME)
                                .short("name")
                                .value_name("NAME")
                                .long("Process_Name")
                                .help("Friendly name for this worker.")
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

    //Initialize logger
    if let Err(ref e) = init_logger(&matches) {
        //Since we failed to initialize the logger, all we can do is log to stdout and exit with a different error code.
        println!("worker_cli failed with the following error: {}", e);
        ::std::process::exit(ERROR_LOG_INITIALIZATION);
    }

    if let Err(ref e) = run(matches) {
        error!("worker_cli failed with the following error: {}", e);
        ::std::process::exit(ERROR_EXECUTION_FAILURE);
    }
}


#[cfg(test)]
mod worker_cli_tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::{self, Write};
    use std::env;

    //Unit test for: create_default_conf_file
    #[test]
    fn test_create_default_conf_file() {
        let file_path = create_default_conf_file().unwrap();
        let md = fs::metadata(file_path).unwrap();
        assert!(md.is_file());
        assert!(md.len() > 0);
    }

    //Unit test for: load_conf_file
    #[test]
    fn test_load_conf_file() {
        //Creating the sample TOML file
        let sample_toml_str = r#"
            worker_name = "Worker1"
            random_seed = 12345
            work_dir = "."
            operation_mode = "Simulated"
            reliability = 1.0
            delay = 0
            scan_interval = 2000
            broadcast_group = "bcast_g1"
        "#;
        let mut file_path = env::temp_dir();
        file_path.push("sample.toml");
        let file_path_str = file_path.to_str().expect("Invalid file path.");
        let mut mock_file = File::create(file_path_str).expect("Error creating toml file.");
        mock_file.write(sample_toml_str.as_bytes()).expect("Error writing to toml file.");
        mock_file.flush().expect("Error flusing toml file to disk.");
        
        //Caling the functiong to test
        let config = load_conf_file(file_path_str);

        //Asserting the returns struct matches the expected values of the sample file.
        let mut mock_config = WorkerConfig::new();
        mock_config.broadcast_group = Some("bcast_g1".to_string());
        mock_config.delay = Some(0);
        mock_config.operation_mode = worker::OperationMode::Simulated;
        mock_config.random_seed = 12345;
        mock_config.reliability = Some(1.0);
        mock_config.scan_interval = Some(2000);
        mock_config.work_dir = ".".to_string();
        mock_config.worker_name = "Worker1".to_string();

        assert_eq!(mock_config, config);
    }

    //Unit test for: create_default_conf_file
    #[test]
    fn test_init_logger() {
        panic!("test failed!");
    }

    //Unit test for: run
    #[test]
    fn test_init_run() {
        panic!("test failed!");
    }

    //Unit test for: decode_worker_data
    #[test]
    fn test_init_decode_worker_data() {
        panic!("test failed!");
    }
    
}