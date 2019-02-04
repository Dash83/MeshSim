extern crate mesh_simulator;
extern crate clap;
extern crate rustc_serialize;
extern crate serde;
extern crate serde_cbor;

#[macro_use]
extern crate slog;

extern crate toml;
extern crate rand;

use mesh_simulator::worker::worker_config::WorkerConfig;
use mesh_simulator::worker;
use mesh_simulator::logging;
use clap::{Arg, App, ArgMatches};
use std::fs::{OpenOptions, File, self};
use std::io::{self, Read};
use std::fmt;
use std::error;
use std::env;
use std::path::Path;
use slog::{Drain, Logger};

const ARG_CONFIG : &'static str = "config";
const ARG_WORKER_NAME : &'static str = "worker_name";
const ARG_WORK_DIR : &'static str = "work_dir";
const ARG_RANDOM_SEED : &'static str = "random_seed";
const ARG_OPERATION_MODE : &'static str = "operation_mode";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");
const CONFIG_FILE_NAME  : &'static str = "worker.toml";
const ERROR_EXECUTION_FAILURE : i32 = 1;
const ERROR_INITIALIZATION : i32 = 2;



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
    Configuration(String),
}

impl error::Error for CLIError {
    fn description(&self) -> &str {
        match *self {
            CLIError::SetLogger(ref err) => err.description(),
            CLIError::IO(ref err) => err.description(),
            CLIError::Worker(ref err) => err.description(),
            CLIError::Serialization(ref err) => err.description(),
            CLIError::TOML(ref err) => err.description(),
            CLIError::Configuration(ref err) => err.as_str(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            CLIError::SetLogger(ref err) => Some(err),
            CLIError::IO(ref err) => Some(err),
            CLIError::Worker(ref err) => Some(err),
            CLIError::Serialization(ref err) => Some(err),
            CLIError::TOML(ref err) => Some(err),
            CLIError::Configuration(_) => None,
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
            CLIError::Configuration(ref err) => write!(f, "TOML error: {}", err),
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

fn run(config : WorkerConfig, logger : Logger) -> Result<(), CLIError> {
   let mut obj =  config.create_worker(logger)?;
   //debug!("Worker Obj: {:?}", obj);
   try!(obj.start());
   Ok(())
}

fn load_conf_file<'a>(file_path : &'a str) -> Result<WorkerConfig, CLIError> {
    //Check that configuration file passed exists.
    //If it doesn't exist, error out
    let mut file = try!(File::open(file_path));
    let mut file_content = String::new();
    //Not checking bytes read since all we can check without scanning the file is that is not empty.
    //The serialization framework however, will do the appropriate validations.
    try!(file.read_to_string(&mut file_content));
    let configuration : WorkerConfig = try!(toml::from_str(&file_content));
    Ok(configuration)
}

fn get_cli_parameters<'a>() -> ArgMatches<'a> {
    App::new("Worker_cli").version(VERSION)
                          .author("Marco Caballero <marco.caballero@cl.cam.ac.uk>")
                          .about("CLI interface to the worker object from the mesh simulator system")
                          .arg(Arg::with_name(ARG_CONFIG)
                                .short("c")
                                .long("config")
                                .value_name("FILE")
                                .help("Configuration file for the worker.")
                                .takes_value(true))
                          .arg(Arg::with_name(ARG_WORKER_NAME)
                                .short("worker")
                                .value_name("NAME")
                                .long("worker_name")
                                .help("Friendly name for this worker.")
                                .takes_value(true))
                          .arg(Arg::with_name(ARG_OPERATION_MODE)
                                .short("mode")
                                .value_name("MODE")
                                .long("operation_mode")
                                .help("Should the worker operte in DEVICE or SIMULATED mode?")
                                .takes_value(true))
                          .arg(Arg::with_name(ARG_RANDOM_SEED)
                                .short("random")
                                .value_name("SEED")
                                .long("random_seed")
                                .help("Random seed usef for all RNG operations.")
                                .takes_value(true))
                          .arg(Arg::with_name(ARG_WORK_DIR)
                                .short("dir")
                                .long("work_dir")
                                .value_name("DIR")
                                .help("Operating directory for the program, where results and logs will be placed.")
                                .takes_value(true))
                          .get_matches()
}

fn validate_config(config : &mut WorkerConfig, matches : &ArgMatches) -> Result<(), CLIError> {
    // Mandatory values should have been validated when loading the TOML file, just check the values are appropriate.
    // Worker_name
    config.worker_name = matches.value_of(ARG_WORKER_NAME).unwrap_or(config.worker_name.as_str()).to_string();
    if config.worker_name.is_empty() {
        eprintln!("worker_name can't be empty.");
        return Err(CLIError::Configuration("Worker name was empty.".to_string()))
    }

    //work_dir
    config.work_dir = matches.value_of(ARG_WORK_DIR).unwrap_or(config.work_dir.as_str()).to_string();
    let dir_info = try!(fs::metadata(std::path::Path::new(config.work_dir.as_str())));
    if !dir_info.is_dir() || dir_info.permissions().readonly() {
        eprintln!("work_dir is not a valid directory or it's not writable.");
        return Err(CLIError::Configuration("work_dir is not a valid directory or it's not writable.".to_string()))
    }

    // Operation_mode and broadcast group
    // If operation_mode is "Simulated", at least 1 broadcast group must be provided.
    let op_mode_param = matches.value_of(ARG_OPERATION_MODE).unwrap_or("");
    if !op_mode_param.is_empty() {
        config.operation_mode = try!(op_mode_param.parse::<worker::OperationMode>());
    }

    Ok(())
}

/// The init process performs all initialization required for the worker_cli.
/// It performs 2 main tasks: read the configuration file and process the command line parameters.
fn init(matches : &ArgMatches) -> Result<WorkerConfig, CLIError> {
    //Read the configuration file.
    let mut current_dir = env::current_dir()?;
    let config_file_path = matches.value_of(ARG_CONFIG).unwrap_or_else(|| {
        //No configuration file was passed. Look for default option: current_dir + default name.
         current_dir.push(CONFIG_FILE_NAME);
         current_dir.to_str().expect("No configuration file was provided and current directory is not readable.")
    });
    let mut configuration = load_conf_file(config_file_path)?;
    
    //Validate the current configuration
    let _res = validate_config(&mut configuration, &matches)?;

    Ok(configuration)
}

fn main() {
    //Get the CLI parameters
    let matches = get_cli_parameters();

    //Initialization
    let config = init(&matches).unwrap_or_else(|e| {
                            println!("worker_cli failed with the following error: {}", e);
                            ::std::process::exit(ERROR_INITIALIZATION);
                        });

    let log_file_name = format!("{}{}{}{}{}.log", &config.work_dir,
                                                std::path::MAIN_SEPARATOR,
                                                logging::LOG_DIR_NAME,
                                                std::path::MAIN_SEPARATOR,
                                                &config.worker_name );
    let logger = logging::create_logger(&log_file_name).unwrap_or_else(|e| {
                            println!("worker_cli failed with the following error: {}", e);
                            ::std::process::exit(ERROR_INITIALIZATION);
    });

    //Main loop
    if let Err(ref e) = run(config, logger.clone()) {
        error!(logger, "worker_cli failed with the following error: {}", e);
        ::std::process::exit(ERROR_EXECUTION_FAILURE);
    }
}


#[cfg(test)]
mod worker_cli_tests {
    use super::*;

    //Unit test for: load_conf_file
    // #[test]
    // fn test_load_conf_file() {
       /*
        This test has been commented out since it the function only makes use of the functionality to deserialize
        a worker_config object. Tests in the worker_config module should cover this.
       */ 
    // }

    // //Unit test for: init_logger
    // #[test]
    // fn test_init_logger() {
    //     let worker_name = "test_worker";
    //     let dir = std::env::temp_dir();
    //     let res = init_logger(dir.to_str().unwrap(), worker_name);
        
    //     assert!(res.is_ok());
    // }

    //Unit test for: run
    // #[test]
    // #[ignore]
    // fn test_run() {
        /*
        This function basically just runs the start method of the worker type. This code will be tested there.
        */
    // }

    //Unit test for: validate_config
    #[test]
    fn test_validate_config() {
        let mut config = WorkerConfig::new();
        //Empty argument matches, as the config should be complete.
        let matches = ArgMatches::new(); 
        let res = validate_config(&mut config, &matches);

        assert!(res.is_ok());
    }
    
    //Unit test for: get_cli_parameters
    // #[test]
    // #[ignore]
    // fn test_get_cli_parameters() {
        /*
         This function only uses clap to parse parameters. There is not much to test but the code of the library.
        */
    // }
}