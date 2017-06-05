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

use mesh_simulator::worker::{Worker};
use mesh_simulator::worker;
use clap::{Arg, App, ArgMatches};
use rustc_serialize::hex::*;
use serde_cbor::de::*;
use slog::DrainExt;
use std::fs::OpenOptions;
use std::io;
use std::fmt;
use std::error;

const ARG_CONFIG : &'static str = "config";
const ARG_WORKER_OBJ : &'static str = "worker_obj";
const ARG_PROC_NAME : &'static str = "proc_name";
const ARG_WORK_DIR : &'static str = "work_dir";

const ERROR_EXECUTION_FAILURE : i32 = 1;
const ERROR_LOG_INITIALIZATION : i32 = 2;


#[derive(Debug)]
enum CLIError {
    SetLogger(log::SetLoggerError),
    IO(io::Error),
    Worker(worker::WorkerError),
    Serialization(serde_cbor::Error),
}

impl error::Error for CLIError {
    fn description(&self) -> &str {
        match *self {
            CLIError::SetLogger(ref err) => err.description(),
            CLIError::IO(ref err) => err.description(),
            CLIError::Worker(ref err) => err.description(),
            CLIError::Serialization(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            CLIError::SetLogger(ref err) => Some(err),
            CLIError::IO(ref err) => Some(err),
            CLIError::Worker(ref err) => Some(err),
            CLIError::Serialization(ref err) => Some(err),
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

fn decode_worker_data<'a>(arg : &'a str) -> Result<Worker, serde_cbor::Error> {
    let e : Vec<u8> = arg.from_hex().unwrap();
    let obj : Result<Worker, _> = from_reader(&e[..]);
    obj
}

fn init_logger(matches : &ArgMatches) -> Result<(), CLIError>  { 
    let working_dir_arg = matches.value_of(ARG_WORK_DIR);
    let proc_name = matches.value_of(ARG_PROC_NAME).unwrap_or("MeshWorker").to_string();

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
    let proc_name = matches.value_of(ARG_PROC_NAME).unwrap_or("MeshWorker");
    
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

fn main() {
    //Build CLI interface
    let matches = App::new("Worker_cli")
                          .version("0.1")
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
                          .arg(Arg::with_name(ARG_PROC_NAME)
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
                          .get_matches();

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