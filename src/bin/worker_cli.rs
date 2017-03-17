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
use clap::{Arg, App};
use rustc_serialize::base64::*;
use std::process;
use serde_cbor::de::*;
use slog::DrainExt;
use std::fs::OpenOptions;

const ARG_CONFIG : &'static str = "config";
const ARG_WORKER_OBJ : &'static str = "worker_obj";
const ARG_PROC_NAME : &'static str = "proc_name";

const ERROR_WORKER_DATA_DECODE : i32 = 1;

fn decode_worker_data<'a>(arg : &'a str) -> Result<Worker, serde_cbor::Error> {
    let e : Vec<u8> = arg.as_bytes().from_base64().unwrap();
    let obj : Result<Worker, _> = from_reader(&e[..]);
    obj
}

fn init_logger(proc_name : Option<String>, working_dir : Option<String> ) { 
    let working_dir = match working_dir {
        Some(dir) => dir,
        None => ".".to_string(),
    };

     let pn = match proc_name {
        Some(pn) => pn,
        None => "MeshWorker".to_string(),
    };
    let log_file_name = working_dir + "/" + &pn + ".log";

    let log_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(log_file_name).unwrap();
    
    let console_drain = slog_term::streamer().build();
    let file_drain = slog_stream::stream(log_file, slog_json::default());
    let logger = slog::Logger::root(slog::duplicate(console_drain, file_drain).fuse(), o!("Process" => pn));
    slog_stdlog::set_logger(logger).unwrap();
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
                          .get_matches();

    //Obtain individual arguments
    let worker_data = matches.value_of(ARG_WORKER_OBJ);
    let proc_name = matches.value_of(ARG_PROC_NAME).unwrap_or("MeshWorker");

    //Initialize logger

    //TODO: This is a temporary hack (lol?) for setting the log folder while I introduce support for config files.
    let log_dir = "./log".to_string(); 
    init_logger(Some(proc_name.to_string()), Some(log_dir));

    
    let mut obj = match worker_data {
        Some(arg) => {
            match decode_worker_data(arg) {
                Ok(decoded_data) => decoded_data,
                Err(e) => {
                    error!("Decoding the worker data failed with error: {}", e);
                    process::exit(ERROR_WORKER_DATA_DECODE);
                },
            }
        },
        None => { 
            //Build worker object and set optional parameters
            Worker::new(proc_name.to_string())
        }, 

    };

    let exit_status = obj.start();
    info!("Worker exited with {:?}", exit_status);
}