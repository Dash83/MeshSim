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
use clap::{Arg, App};
use std::str::FromStr;
use std::thread;
use slog::DrainExt;
use std::fs::OpenOptions;

const ARG_CONFIG : &'static str = "config";
const ARG_TEST : &'static str = "test";

enum MeshTests {
    BasicTest,
}

impl FromStr for MeshTests {
    type Err = ();

    fn from_str(s : &str) -> Result<MeshTests, ()> {
        match s {
            "BasicTest" => Ok(MeshTests::BasicTest),
            _ => Err(()),
        }
    }
}

fn init_logger(proc_name : Option<String>, working_dir : Option<String> ) { 
    let working_dir = match working_dir {
        Some(dir) => dir,
        None => ".".to_string(),
    };

     let pn = match proc_name {
        Some(pn) => pn,
        None => "MeshMaster".to_string(),
    };
    let log_file_name = working_dir + "/" + &pn + ".log";

    let log_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(false)
                    .open(log_file_name).unwrap();
    
    let console_drain = slog_term::streamer().build();
    let file_drain = slog_stream::stream(log_file, slog_json::default());
    let logger = slog::Logger::root(slog::duplicate(console_drain, file_drain).fuse(), o!());
    slog_stdlog::set_logger(logger).unwrap();
} 

fn test_basic_test() {
    info!("Running BasicTest");
    let mut master = Master::new();
    let w1 = Worker::new("Worker1".to_string());
    let mut w2 = Worker::new("Worker2".to_string());
    w2.add_peers(vec![w1.me.clone()]);

    master.add_worker(w1);
    //Super fucking hacky. It seems the order for process start is not that deterministic.
    //TODO: Find a way to address this.
    thread::sleep(std::time::Duration::from_millis(1000)); 
    master.add_worker(w2);

    match master.wait_for_workers() {
        Ok(_) => info!("Finished successfully."),
        Err(e) => error!("Master failed to wait for children processes with error {}", e),
    }
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
                          .get_matches();

    //Obtain individual arguments
    let test_str = matches.value_of(ARG_TEST);

    //TODO: This is a temporary hack (lol?) for setting the log folder while I introduce support for config files.
    let log_dir = "./log".to_string(); 
    init_logger(None, Some(log_dir));

    match test_str {
        Some(data) => { 
            let selected_test = data.parse::<MeshTests>().unwrap();
            match selected_test {
                MeshTests::BasicTest => { 
                    test_basic_test();
                },
            }
        },
        None => { 
            //Print usage / Test_list
        },
    }
}