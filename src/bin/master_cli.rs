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
use mesh_simulator::master::*;
use mesh_simulator::master;
use clap::{Arg, App, ArgMatches};
use std::str::FromStr;
use std::thread;
use slog::DrainExt;
use std::fs::OpenOptions;
use std::io;
use std::error;
use std::fmt;

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


fn init_logger(matches : &ArgMatches) -> Result<(), CLIError> { 
    let working_dir_arg = matches.value_of(ARG_WORK_DIR);

    let working_dir = match working_dir_arg {
        Some(dir) => dir,
        //TODO: If no working dir parameter is passed, read from config file before defaulting to "."
        None => ".",
    };
    let log_dir = working_dir.to_string() + "/log";
    let pn = "MeshMaster".to_string();
    let log_file_name = log_dir + "/" + &pn + ".log";

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
    let w1 = Worker::new("Worker1".to_string());
    let mut w2 = Worker::new("Worker2".to_string());
    w2.add_peers(vec![w1.me.clone()]);

    try!(master.add_worker(w1));
    //Super fucking hacky. It seems the order for process start is not that deterministic.
    //TODO: Find a way to address this.
    thread::sleep(std::time::Duration::from_millis(2000)); 
    try!(master.add_worker(w2));

    match master.wait_for_workers() {
        Ok(_) => info!("Finished successfully."),
        Err(e) => error!("Master failed to wait for children processes with error {}", e),
    }
    Ok(())
}

/// This test creates 
fn test_six_node_test() -> Result<(), CLIError> {
    info!("Running BasicTest");
    let mut master = Master::new();
    let w1 = Worker::new("Worker1".to_string());
   
    let mut w2 = Worker::new("Worker2".to_string());
    w2.add_peers(vec![w1.me.clone()]);
   
    let mut w3 = Worker::new("Worker3".to_string());
    w3.add_peers(vec![w2.me.clone()]);

    let mut w4 = Worker::new("Worker4".to_string());
    w4.add_peers(vec![w3.me.clone()]);

    let mut w5 = Worker::new("Worker5".to_string());
    w5.add_peers(vec![w4.me.clone()]);

    let mut w6 = Worker::new("Worker6".to_string());
    w6.add_peers(vec![w5.me.clone()]);

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
    Ok(())
}

fn print_usage() {

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
            //Print usage / Test_list
            print_usage();
        },
    }
    Ok(())
}

fn main() {
        //Build CLI interface
    let matches = App::new("Master_cli")
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
                          .get_matches();

    if let Err(ref e) = init_logger(&matches) {
        //Since we failed to initialize the logger, all we can do is log to stdout and exit with a different error code.
        println!("master_cli failed with the following error: {}", e);
        ::std::process::exit(ERROR_LOG_INITIALIZATION);
    }

    if let Err(ref e) = run(matches) {
        error!("master_cli failed with the following error: {}", e);
        ::std::process::exit(ERROR_EXECUTION_FAILURE);
    }
}