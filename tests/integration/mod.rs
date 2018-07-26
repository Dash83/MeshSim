extern crate assert_cli;
extern crate chrono;
extern crate mesh_simulator;

use self::chrono::prelude::*;
use std::path::{PathBuf, Path};
use std::env;
use std::fs;
use self::mesh_simulator::logging;


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
    let test_home = env::var("MESHIM_TEST_DIR").unwrap_or(String::from("/tmp/Tests/"));
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

/****************** Platform *****************/
mod device_mode;

/****************** Protocols ****************/
mod tmembership;