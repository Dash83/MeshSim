use clap::{App, Arg, ArgMatches};
use mesh_simulator::{master, worker};
use mesh_simulator::master::*;
use mesh_simulator::{MeshSimError, MeshSimErrorKind};

// use slog::DrainExt;

use mesh_simulator::logging;
use slog::Logger;
use std::error;
use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::time::Duration;
#[macro_use]
extern crate slog;

//const ARG_CONFIG : &'static str = "config";
const ARG_WORK_DIR: &str = "work_dir";
const ARG_TEST_FILE: &str = "test_file";
const ARG_WORKER_PATH: &str = "worker_path";
const ARG_TERMINAL_LOG: &str = "term_log";
const ARG_SLEEP_TIME: &str = "sleep_time";
const ARG_DB_NAME: &str = "database_name";
const ARG_RANDOM_SEED: &str = "random_seed";

const ERROR_LOG_INITIALIZATION: i32 = 1;
const ERROR_EXECUTION_FAILURE: i32 = 2;

#[derive(Debug)]
enum CLIError {
    #[allow(unused)]
    SetLogger(String),
    IO(io::Error),
    Master(master::MasterError),
    #[allow(unused)]
    TestParsing(String),
}

impl error::Error for CLIError {
    // fn description(&self) -> &str {
    //     match *self {
    //         CLIError::SetLogger(ref desc) => &desc,
    //         CLIError::IO(ref err) => err.description(),
    //         CLIError::Master(ref err) => err.description(),
    //         CLIError::TestParsing(ref err_str) => err_str,
    //     }
    // }

    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            CLIError::SetLogger(_) => None,
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
    fn from(err: io::Error) -> CLIError {
        CLIError::IO(err)
    }
}

impl From<master::MasterError> for CLIError {
    fn from(err: master::MasterError) -> CLIError {
        CLIError::Master(err)
    }
}

fn run(mut master: Master, matches: &ArgMatches) -> Result<(), MeshSimError> {
    //Has a test file been provided?
    let test_file = matches.value_of(ARG_TEST_FILE);
    let sleep_time_override = matches.value_of(ARG_SLEEP_TIME);
    let master_log_file = master.log_file.clone();
    if let Some(file) = test_file {
        let test_spec = test_specification::TestSpec::parse_test_spec(file)?;
        master.duration = test_spec.duration;
        master.test_area.height = test_spec.area_size.height;
        master.test_area.width = test_spec.area_size.width;
        master.mobility_model = test_spec.mobility.clone();
        master.run_test(test_spec, sleep_time_override)?;
    }

    //Temporary fix. Since the logging now runs in a separate thread, the tests may end before
    //all the data is captured. Check if the log file is still changing.
    let md = std::fs::metadata(&master_log_file).map_err(|e| {
        let err_msg = String::from("Failed read log file");
        MeshSimError {
            kind: MeshSimErrorKind::Master(err_msg),
            cause: Some(Box::new(e)),
        }
    })?;
    let mut size1 = md.len();
    let max_wait = 20;
    for _i in 0..max_wait {
        std::thread::sleep(Duration::from_millis(100));
        let size2 = md.len();
        if size1 < size2 {
            //File is still changing
            size1 = size2;
        } else {
            //We are good. Exit.
            break;
        }
    }
    Ok(())
}

fn init(matches: &ArgMatches) -> Result<(Master, Logger), MeshSimError> {
    //Determine the work_dir
    let work_dir = match matches.value_of(ARG_WORK_DIR) {
        Some(arg) => String::from(arg),
        None => String::from("."),
    };

    //Should we log to the terminal
    let log_term: bool = matches
        .value_of(ARG_TERMINAL_LOG)
        .unwrap_or("false")
        .parse()
        .unwrap_or(false);

    //if the workdir does not exists, create it
    let mut work_dir_path = PathBuf::from(&work_dir);
    if !work_dir_path.exists() {
        std::fs::create_dir(&work_dir_path).map_err(|e| {
            let err_msg = String::from("Failed create work directory");
            MeshSimError {
                kind: MeshSimErrorKind::Master(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    }

    //Make sure the log directory exists
    work_dir_path.push(logging::LOG_DIR_NAME);
    if !work_dir_path.exists() {
        std::fs::create_dir(&work_dir_path)
        .map_err(|e| {
            let err_msg = String::from("Failed create log directory");
            MeshSimError {
                kind: MeshSimErrorKind::Master(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    }
    work_dir_path.pop();

    //Make sure the socket directory exists
    work_dir_path.push(worker::SOCKET_DIR);
    if !work_dir_path.exists() {
        std::fs::create_dir(&work_dir_path)
        .map_err(|e| {
            let err_msg = String::from("Failed create sockets directory");
            MeshSimError {
                kind: MeshSimErrorKind::Master(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    }
    work_dir_path.pop();

    work_dir_path.push(logging::LOG_DIR_NAME);
    work_dir_path.push(logging::DEFAULT_MASTER_LOG);

    let log_file = work_dir_path.to_str().unwrap().into();
    let logger = logging::create_logger(&log_file, log_term).map_err(|e| {
        let err_msg = String::from("Failed to set the logger for the Master.");
        MeshSimError {
            kind: MeshSimErrorKind::Master(err_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    //If no DB name is passed, take the work directory name as the db name.
    let db_name = matches
        .value_of(ARG_DB_NAME)
        .map(|x| x.into())
        .unwrap_or_else(|| { 
            let parts: Vec<&str> = work_dir
            .as_str()
            .rsplit(std::path::MAIN_SEPARATOR)
            .collect();
            parts[0].to_string()
        });
    
    //If a random seed is passed, use it.
    let random_seed = matches
        .value_of(ARG_RANDOM_SEED)
        .map(|v| v.parse::<u64>())
        .transpose()
        .map_err(|e| { 
            let err = "Could not parse provided random_seed into u64".into();
            MeshSimError {
                kind: MeshSimErrorKind::Configuration(err),
                cause: Some(Box::new(e)),
            }
        })?;

    let worker_path = matches.value_of(ARG_WORKER_PATH).or(Some("./worker_cli")).unwrap().into();
    let master = Master::new(work_dir, worker_path, log_file, db_name, random_seed, logger.clone())?;

    Ok((master, logger))
}

fn get_cli_parameters<'a>() -> ArgMatches<'a> {
    App::new("Master_cli")
        .version("0.1")
        .author("Marco Caballero <marco.caballero@cl.cam.ac.uk>")
        .about("CLI interface to the Master object from the mesh simulator system")
        .arg(
            Arg::with_name(ARG_WORK_DIR)
                .short("dir")
                .value_name("WORK_DIR")
                .help("Operating directory for the program, where results and logs will be placed.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(ARG_WORKER_PATH)
                .short("worker")
                .value_name("WORKER_PATH")
                .help("Absolute path to the worker binary.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(ARG_TEST_FILE)
                .short("test_file")
                .value_name("FILE")
                .help("File that contains a valid test specification for the master to run.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(ARG_TERMINAL_LOG)
                .short("log_term")
                .long("log_to_terminal")
                .value_name("true/false")
                .help("Should this worker log operations to the terminal as well")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(ARG_SLEEP_TIME)
                .short("sleep")
                .long("worker_sleep_time")
                .value_name("MILLISECONDS")
                .help("Override the sleep time of workers specified in the test file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(ARG_DB_NAME)
                .short("name")
                .long("db_name")
                .value_name("NAME")
                .help("Name for the database that will be created for running the test spec passed")
                .takes_value(true),
        )
        .arg(
            Arg::with_name(ARG_RANDOM_SEED)
                .short("rand")
                .long("random_seed")
                .value_name("u64")
                .help("Random seed used for creating the RNG for the Master")
                .takes_value(true),
        )        
        .get_matches()
}

fn main() {
    //Enable the a more readable version of backtraces
    color_backtrace::install();

    //Build CLI interface
    let matches = get_cli_parameters();
    let (master, logger) = init(&matches).unwrap_or_else(|e| {
        eprintln!("master_cli failed with the following error: {}", e);
        if let Some(cause) = e.cause {
            eprintln!("cause: {}", cause);
        }
        ::std::process::exit(ERROR_LOG_INITIALIZATION);
    });

    if let Err(ref e) = run(master, &matches) {
        error!(&logger, "master_cli failed with the following error: {}", e);
        error!(&logger, "Error chain: ");
        let mut chain = e.source();
        while let Some(internal) = chain {
            error!(&logger, "Internal error: {}", internal);
            chain = internal.source();
        }

        // Wait for the logger to flush its messages and then exit with an exit code
        std::thread::sleep(Duration::from_millis(1000));
        ::std::process::exit(ERROR_EXECUTION_FAILURE);
    }

}
