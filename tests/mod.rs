
extern crate mesh_simulator;
extern crate chrono;
extern crate assert_cli;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;
#[macro_use]
extern crate lazy_static;

use self::chrono::prelude::*;
use std::path::{PathBuf, Path};
use std::env;
use std::fs::{self};
use self::mesh_simulator::logging::{self, *};
use std::{sync::Mutex};
use std::process::{Command, Stdio};

/***********************************************/
/**************** Shared Resources *************/
/***********************************************/
lazy_static! {
    static ref WIRELESS_NIC: Mutex<()> = Mutex::new(());
}

type TestResult<T = ()> = std::result::Result<T, Box<std::error::Error>>;

/***********************************************/
/**************** Helper functions *************/
/***********************************************/
fn get_test_specification_dir() -> PathBuf {
    env::current_dir()
            .expect("Couldn't get current dir")
            .join("tests")
            .join("integration")
            .join("specs")            
}

fn get_master_path() -> String {
    let file_pb = env::current_dir()
                    .expect("Couldn't get current dir")
                    .join("target")
                    .join("debug")
                    .join("master_cli");
    format!("{}", file_pb.display())
}

fn get_worker_path() -> String {
    let file_pb = env::current_dir()
                    .expect("Couldn't get current dir")
                    .join("target")
                    .join("debug")
                    .join("worker_cli");
    format!("{}", file_pb.display())
}

fn get_test_path<'a>(test : &'a str) -> String {
    let file_pb = get_test_specification_dir()
                        .join(test);
    format!("{}", file_pb.display())
}

//MESHIM_TEST_DIR should point to "/media/marco/Data/Tests/" at my lab computer.
fn get_tests_root() -> String {
    let test_home = env::var("MESHSIM_TEST_DIR").unwrap_or(String::from("/tmp/"));
    test_home
}

fn create_test_dir<'a>(test_name : &'a str) -> String {
    let now : DateTime<Utc> = Utc::now();
    let test_dir_path = format!("{}{}_{}", &get_tests_root(), 
                                             test_name,
                                             now.timestamp());
    let test_dir = Path::new(&test_dir_path);

    if !test_dir.exists() {
        fs::create_dir(&test_dir_path).expect(&format!("Unable to create test results directory {}", test_dir_path));
    }

    test_dir_path.clone()
}

//********************************
//**** RGR utility Functions *****
//********************************
fn count_data_packets(log_recors: &Vec<LogEntry>) -> usize {
    let mut packet_count = 0;
    for record in log_recors.iter() {
        if record.status.is_some() && record.msg_type.is_some() {
            let status = &record.status.clone().unwrap();
            let msg_type = &record.msg_type.clone().unwrap();
            if status == "ACCEPTED" && msg_type == "DATA" {
                packet_count += 1;
            }
        } 
    }
    packet_count
}

// fn count_all_packets<P: AsRef<Path>>(pattern: &str, dir: &str) -> usize {
fn count_all_packets(pattern: &str, dir: &str) -> usize {
    let mut targets = PathBuf::from(dir);
    targets.push("*.log");
    let targets = targets.to_str().expect("Could not encode DIR argument to UTF-8");
    let ls_child = Command::new("ls")
        .arg(targets)
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to get list of files");
    let files = ls_child.stdout.expect("Failed to get output from ls");
    
    let grep_child = Command::new("grep")
        .arg("-ci")
        .arg(pattern)
        .stdin(Stdio::from(files))
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to run grep process");
    let output = grep_child.wait_with_output().expect("Failed to get grep output");
    println!("{:?}", output);

    unimplemented!("Not yet!")
}

mod experiments;
mod integration;
mod unit;
