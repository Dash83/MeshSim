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

use mesh_simulator::master::*;
use mesh_simulator::master;
use clap::{Arg, App, ArgMatches};
use std::str::FromStr;
use slog::DrainExt;
use std::fs::{OpenOptions};
use std::path::Path;
use std::io;
use std::error;
use std::fmt;
use std::error::Error;

//const ARG_CONFIG : &'static str = "config";
const ARG_WORK_DIR : &'static str = "work_dir";
const ARG_TEST_FILE : &'static str = "test_file";
const ARG_WORKER_PATH : &'static str = "worker_path";

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

fn run(mut master : Master, matches : &ArgMatches) -> Result<(), CLIError> {    
    //Has a test file been provided?
    let test_file = matches.value_of(ARG_TEST_FILE); 
    if let Some(file) = test_file {
        let mut test_spec = try!(test_specification::TestSpec::parse_test_spec(file));
        try!(master.run_test(test_spec));
    }

    Ok(())
}

fn init(matches : &ArgMatches) -> Result<(Master), CLIError> {
    //TODO: Implement configuration file support for the master.
    //Read the configuration file.
    // let mut current_dir = try!(env::current_dir());
    // let config_file_path = matches.value_of(ARG_CONFIG).unwrap_or_else(|| {
    //     //No configuration file was passed. Look for default option: current_dir + default name.
    //      current_dir.push(CONFIG_FILE_NAME);
    //      current_dir.to_str().expect("No configuration file was provided and current directory is not readable.")
    // });
    // let mut configuration = try!(load_conf_file(config_file_path));
    let mut master = Master::new();

    //Initialize logger
    let work_dir = match matches.value_of(ARG_WORK_DIR) {
        Some(arg) => String::from(arg),
        None => master.work_dir,
    };

    master.work_dir = work_dir.clone();

    let work_dir_path = Path::new(&work_dir);
    if !work_dir_path.exists() {
        let _ = try!(std::fs::create_dir(&work_dir));
    }
    try!(init_logger(work_dir_path));

    //What else was passed to the master?
    master.worker_binary = match matches.value_of(ARG_WORKER_PATH) {
                                Some(path_arg) => String::from(path_arg),
                                None => master.worker_binary,
                                };

    Ok(master)
}

fn get_cli_parameters<'a>() -> ArgMatches<'a> {
    App::new("Master_cli")
            .version("0.1")
            .author("Marco Caballero <marco.caballero@cl.cam.ac.uk>")
            .about("CLI interface to the Master object from the mesh simulator system")
            .arg(Arg::with_name(ARG_WORK_DIR)
                .short("dir")
                .value_name("WORK_DIR")
                .help("Operating directory for the program, where results and logs will be placed.")
                .takes_value(true))
            .arg(Arg::with_name(ARG_WORKER_PATH)
                .short("worker")
                .value_name("WORKER_PATH")
                .help("Absolute path to the worker binary.")
                .takes_value(true))
            .arg(Arg::with_name(ARG_TEST_FILE)
                .short("test_file")
                .value_name("FILE")
                .help("File that contains a valid test specification for the master to run.")
                .takes_value(true))
            .get_matches()
}

fn main() {
    //Build CLI interface
    let matches = get_cli_parameters();
    
    let master = init(&matches).unwrap_or_else(|e| {
                                    println!("master_cli failed with the following error: {}", e);
                                    ::std::process::exit(ERROR_LOG_INITIALIZATION);
                                });

    if let Err(ref e) = run(master, &matches) {
        error!("master_cli failed with the following error: {}", e);
        error!("Error chain: ");
        let mut chain = e.cause();
        while let Some(internal) = chain {
            error!("Internal error: {}", internal);
            chain = internal.cause();
        }

        ::std::process::exit(ERROR_EXECUTION_FAILURE);
    }
}