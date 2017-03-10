extern crate mesh_simulator;
extern crate clap;
extern crate rustc_serialize;
extern crate serde;
extern crate serde_cbor;

use mesh_simulator::worker::{Worker, Peer};
use mesh_simulator::master::*;
use clap::{Arg, App};
use rustc_serialize::base64::*;
use std::process;
use serde_cbor::de::*;
use serde_cbor::Error;
use std::str::FromStr;

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

fn test_BasicTest() {
    let mut master = Master::new();
    let mut w1 = Worker::new("Worker1".to_string());
    let mut w2 = Worker::new("Worker2".to_string());
    w2.add_peers(vec![w1.me.clone()]);

    master.add_worker(w1);
    master.add_worker(w2);

    match master.wait_for_workers() {
        Ok(_) => println!("Finished successfully."),
        Err(e) => println!("Master failed with process"),
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
    let config_file = matches.value_of(ARG_CONFIG);
    let test_str = matches.value_of(ARG_TEST);

    match test_str {
        Some(data) => { 
            let selected_test = data.parse::<MeshTests>().unwrap();
            match selected_test {
                MeshTests::BasicTest => { 
                    test_BasicTest();
                },
            }
        },
        None => { 
            //Print usage / Test_list
        },
    }
}