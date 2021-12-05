#[macro_use]
extern crate slog;
extern crate mesh_simulator;

use clap::{App, Arg, ArgMatches};
use mesh_simulator::worker::worker_config::WorkerConfig;
use mesh_simulator::{logging, worker};
use mesh_simulator::{MeshSimError, MeshSimErrorKind};
// use slog::Logger;
use std::fs::{self, File};
use std::io::{self, Read};
use std::{env, error, fmt};

const ARG_CONFIG: &str = "config";
const ARG_WORKER_NAME: &str = "worker_name";
const ARG_WORK_DIR: &str = "work_dir";
const ARG_REGISTER_WORKER: &str = "register_worker";
const ARG_ACCEPT_COMMANDS: &str = "accept_commands";
const ARG_TERMINAL_LOG: &str = "term_log";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const CONFIG_FILE_NAME: &str = "worker.toml";
// const ERROR_EXECUTION_FAILURE: i32 = 1;
const ERROR_INITIALIZATION: i32 = 2;

// *****************************************
// ************ Module Errors **************
// *****************************************

#[derive(Debug)]
enum CLIError {
    #[allow(unused)]
    SetLogger(String),
    IO(io::Error),
    Worker(worker::WorkerError),
    Serialization(bincode::Error),
    TOML(toml::de::Error),
    #[allow(unused)]
    Configuration(String),
}

impl error::Error for CLIError {
    // fn description(&self) -> &str {
    //     match *self {
    //         CLIError::SetLogger(ref desc) => &desc,
    //         CLIError::IO(ref err) => err.description(),
    //         CLIError::Worker(ref err) => err.description(),
    //         CLIError::Serialization(ref err) => err.description(),
    //         CLIError::TOML(ref err) => err.description(),
    //         CLIError::Configuration(ref err) => err.as_str(),
    //     }
    // }

    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            CLIError::SetLogger(_) => None,
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
    fn from(err: io::Error) -> CLIError {
        CLIError::IO(err)
    }
}

// impl From<log::SetLoggerError> for CLIError {
//     fn from(err : log::SetLoggerError) -> CLIError {
//         CLIError::SetLogger(err)
//     }
// }

impl From<bincode::Error> for CLIError {
    fn from(err: bincode::Error) -> CLIError {
        CLIError::Serialization(err)
    }
}

impl From<worker::WorkerError> for CLIError {
    fn from(err: worker::WorkerError) -> CLIError {
        CLIError::Worker(err)
    }
}

impl From<toml::de::Error> for CLIError {
    fn from(err: toml::de::Error) -> CLIError {
        CLIError::TOML(err)
    }
}
// ***************End Errors****************

fn run(config: WorkerConfig) -> Result<(), MeshSimError> {
    let log_file_name = format!(
        "{}{}{}{}{}.log",
        &config.work_dir,
        std::path::MAIN_SEPARATOR,
        logging::LOG_DIR_NAME,
        std::path::MAIN_SEPARATOR,
        &config.worker_name
    );
    let logger = logging::create_logger(&log_file_name, config.term_log.unwrap_or(false))
        .unwrap_or_else(|e| {
            println!("worker_cli failed with the following error: {}", e);
            ::std::process::exit(ERROR_INITIALIZATION);
        });

    info!(logger, "Worker Config: {:?}", &config);
    let ac = config.accept_commands.unwrap_or(false);
    let mut obj = config.create_worker(logger)?;
    //debug!("Worker Obj: {:?}", obj);
    obj.start(ac)?;
    Ok(())
}

fn load_conf_file(file_path: &str) -> Result<WorkerConfig, MeshSimError> {
    let mut file_content = String::new();
    let mut file = File::open(file_path).map_err(|e| {
        let err_msg = String::from("Failed to open configuration file");
        MeshSimError {
            kind: MeshSimErrorKind::Configuration(err_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    file.read_to_string(&mut file_content).map_err(|e| {
        let err_msg = String::from("Failed to read configuration file");
        MeshSimError {
            kind: MeshSimErrorKind::Configuration(err_msg),
            cause: Some(Box::new(e)),
        }
    })?;
    let configuration: WorkerConfig = toml::from_str(&file_content).map_err(|e| {
        let err_msg = String::from("Failed to deserialize configuration file");
        MeshSimError {
            kind: MeshSimErrorKind::Serialization(err_msg),
            cause: Some(Box::new(e)),
        }
    })?;
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
                          .arg(Arg::with_name(ARG_WORK_DIR)
                                .short("dir")
                                .long("work_dir")
                                .value_name("DIR")
                                .help("Operating directory for the program, where results and logs will be placed.")
                                .takes_value(true))
                          .arg(Arg::with_name(ARG_REGISTER_WORKER)
                                .short("register")
                                .long("register_worker")
                                .value_name("true/false")
                                .help("Should the worker register in the DB. Option should be NO when started by the Master.")
                                .takes_value(true))
                          .arg(Arg::with_name(ARG_ACCEPT_COMMANDS)
                                .short("accept")
                                .long("accept_commands")
                                .value_name("true/false")
                                .help("Should this worker start a thread to listen for commands")
                                .takes_value(true))
                          .arg(Arg::with_name(ARG_TERMINAL_LOG)
                                .short("log_term")
                                .long("log_to_terminal")
                                .value_name("true/false")
                                .help("Should this worker log operations to the terminal as well")
                                .takes_value(true))
                          .get_matches()
}

fn validate_config(config: &mut WorkerConfig, matches: &ArgMatches) -> Result<(), MeshSimError> {
    // Mandatory values should have been validated when loading the TOML file, just check the values are appropriate.
    // Worker_name
    config.worker_name = matches
        .value_of(ARG_WORKER_NAME)
        .unwrap_or_else(|| config.worker_name.as_str())
        .to_string();
    if config.worker_name.is_empty() {
        eprintln!("worker_name can't be empty.");
        let err_msg = String::from("Worker name was empty");
        let error = MeshSimError {
            kind: MeshSimErrorKind::Configuration(err_msg),
            cause: None,
        };
        return Err(error);
    }

    //work_dir
    config.work_dir = matches
        .value_of(ARG_WORK_DIR)
        .unwrap_or_else(|| config.work_dir.as_str())
        .to_string();
    fs::metadata(std::path::Path::new(config.work_dir.as_str()))
        .map(|dir_info| {
            assert!(dir_info.is_dir());
            assert!(!dir_info.permissions().readonly());
        })
        .map_err(|e| {
            let err_msg = String::from("work_dir is not a valid directory or it's not writable");
            MeshSimError {
                kind: MeshSimErrorKind::Configuration(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    // Accept commands
    config.accept_commands = matches
        .value_of(ARG_ACCEPT_COMMANDS)
        .map(|v| v.parse::<bool>().unwrap_or(false))
        .or(config.accept_commands);

    // Log to terminal
    config.term_log = matches
        .value_of(ARG_TERMINAL_LOG)
        .map(|v| v.parse::<bool>().unwrap_or(false))
        .or(config.term_log);

    Ok(())
}

/// The init process performs all initialization required for the worker_cli.
/// It performs 2 main tasks: read the configuration file and process the command line parameters.
fn init(matches: &ArgMatches) -> Result<WorkerConfig, MeshSimError> {
    //Read the configuration file.
    let mut current_dir = env::current_dir().map_err(|e| {
        let err_msg = String::from("Failed to read current directory");
        MeshSimError {
            kind: MeshSimErrorKind::Configuration(err_msg),
            cause: Some(Box::new(e)),
        }
    })?;
    let config_file_path = matches.value_of(ARG_CONFIG).unwrap_or_else(|| {
        //No configuration file was passed. Look for default option: current_dir + default name.
        current_dir.push(CONFIG_FILE_NAME);
        current_dir
            .to_str()
            .expect("No configuration file was provided and current directory is not readable.")
    });
    let mut configuration = load_conf_file(config_file_path)?;
    //Validate the current configuration
    validate_config(&mut configuration, &matches)?;

    Ok(configuration)
}

fn main() {
    //Enable the a more readable version of backtraces
    color_backtrace::install();

    //Get the CLI parameters
    let matches = get_cli_parameters();

    //Initialization
    let config = init(&matches).unwrap_or_else(|e| {
        println!("worker_cli failed with the following error: {}", e);
        std::process::exit(ERROR_INITIALIZATION);
    });

    //Main loop
    if let Err(ref e) = run(config) {
        eprintln!("worker_cli failed with the following error: {}", e);
        // std::process::exit(ERROR_EXECUTION_FAILURE);
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
    //Used to test the output of colored panic.
    #[test]
    #[should_panic]
    fn test_colored_panic() {
        color_backtrace::install();
        panic!("This is supposed to panic!");
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
