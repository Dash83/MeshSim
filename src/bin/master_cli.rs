use clap::{App, Arg, ArgMatches};
use mesh_simulator::master;
use mesh_simulator::master::*;
use mesh_simulator::{MeshSimError, MeshSimErrorKind};

// use slog::DrainExt;

use mesh_simulator::logging;
use std::error;
use std::error::Error;
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

//const ARG_CONFIG : &'static str = "config";
const ARG_WORK_DIR: &str = "work_dir";
const ARG_TEST_FILE: &str = "test_file";
const ARG_WORKER_PATH: &str = "worker_path";
const ARG_TERMINAL_LOG: &str = "term_log";

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
    fn description(&self) -> &str {
        match *self {
            CLIError::SetLogger(ref desc) => &desc,
            CLIError::IO(ref err) => err.description(),
            CLIError::Master(ref err) => err.description(),
            CLIError::TestParsing(ref err_str) => err_str,
        }
    }

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

// fn init_logger(work_dir : &Path) -> Result<(), CLIError> {
//     let mut log_dir_buf = work_dir.to_path_buf();
//     log_dir_buf.push("log");
//     if !log_dir_buf.exists() {
//         try!(std::fs::create_dir(log_dir_buf.as_path()));
//     }

//     log_dir_buf.push("MeshMaster.log");
//     //At this point the log folder has been created, so the path should be valid.
//     let log_file_name = log_dir_buf.to_str().unwrap();

//     let log_file = try!(OpenOptions::new()
//                         .create(true)
//                         .write(true)
//                         .truncate(true)
//                         .open(log_file_name));
//     // let console_drain = slog_term::streamer().build();
//     // let file_drain = slog_stream::stream(log_file, slog_json::default());
//     // let logger = slog::Logger::root(slog::duplicate(console_drain, file_drain).fuse(), o!());
//     // try!(slog_stdlog::set_logger(logger));

//     Ok(())
// }

fn run(mut master: Master, matches: &ArgMatches) -> Result<(), MeshSimError> {
    //Has a test file been provided?
    let test_file = matches.value_of(ARG_TEST_FILE);
    let logger = master.logger.clone();
    if let Some(file) = test_file {
        let test_spec = test_specification::TestSpec::parse_test_spec(file)?;
        master.duration = test_spec.duration;
        master.test_area.height = test_spec.area_size.height;
        master.test_area.width = test_spec.area_size.width;
        master.mobility_model = test_spec.mobility_model.clone();
        master.run_test(test_spec, &logger)?;
    }

    //Temporary fix. Since the logging now runs in a separate thread, the tests may end before
    //all the data is captured. Check if the log file is still changing.
    let md = std::fs::metadata(&master.log_file).map_err(|e| {
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

fn init(matches: &ArgMatches) -> Result<(Master), MeshSimError> {
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
    let mut master = Master::new(logger, log_file);
    master.work_dir = work_dir.clone();
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
        .get_matches()
}

fn main() {
    //Enable the a more readable version of backtraces
    color_backtrace::install();

    //Build CLI interface
    let matches = get_cli_parameters();
    let master = init(&matches).unwrap_or_else(|e| {
        eprintln!("master_cli failed with the following error: {}", e);
        ::std::process::exit(ERROR_LOG_INITIALIZATION);
    });

    if let Err(ref e) = run(master, &matches) {
        eprintln!("master_cli failed with the following error: {}", e);
        eprintln!("Error chain: ");
        let mut chain = e.source();
        while let Some(internal) = chain {
            eprintln!("Internal error: {}", internal);
            chain = internal.source();
        }

        ::std::process::exit(ERROR_EXECUTION_FAILURE);
    }
}
