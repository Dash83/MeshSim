
extern crate mesh_simulator;
extern crate chrono;
extern crate assert_cli;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;

#[macro_use]
extern crate slog;
extern crate slog_stream;
extern crate slog_term;
extern crate slog_json;
extern crate slog_stdlog;

#[macro_use]
extern crate lazy_static;

use self::chrono::prelude::*;
use std::path::{PathBuf, Path};
use std::env;
use std::fs::{OpenOptions, self};
use self::mesh_simulator::logging;
use slog::DrainExt;
use std::{sync::Mutex, thread::sleep, time::Duration};


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
    use std::env;
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

fn init_logger<'a>(work_dir : &'a str, log_file_name : &'a str) { 
    let log_dir_name = format!("{}{}{}", work_dir, std::path::MAIN_SEPARATOR, "log");
    let log_dir = Path::new(log_dir_name.as_str());
    
    if !log_dir.exists() {
        std::fs::create_dir(log_dir).expect("Could not create log directory.");
    }

    let mut log_dir_buf = log_dir.to_path_buf();
    log_dir_buf.push(format!("{}{}", log_file_name, ".log"));
    
    //At this point the log folder has been created, so the path should be valid.
    let log_file_name = log_dir_buf.to_str().unwrap();

    let log_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(log_file_name).expect("Could not create log file.");
    
    let console_drain = slog_term::streamer().build();
    let file_drain = slog_stream::stream(log_file, slog_json::default());
    let logger = slog::Logger::root(slog::duplicate(console_drain, file_drain).fuse(), o!());
    slog_stdlog::set_logger(logger).expect("Failed to set global logger.");
} 


mod experiments;
mod integration;
mod unit;
