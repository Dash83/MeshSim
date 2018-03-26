extern crate assert_cli;
extern crate chrono;
use self::chrono::prelude::*;

use std::path::{self, PathBuf, Path};
use std::env;
use std::fs;

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

fn get_tests_root() -> String {
    String::from("/media/marco/Data/Tests/")
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
/***********************************************/
/********************* Tests *******************/
/***********************************************/
#[test]
fn integration_basic_test() {
    let test = get_test_path("basic_test.toml");
    let program = get_master_path();
    let worker = get_worker_path();
    let work_dir = create_test_dir("basic_test");

    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    assert_cli::Assert::command(&[&program])
    .with_args(&["-t",  &test, "-w", &worker, "-d", &work_dir])
    .succeeds()
    .and()
    .stdout()
    .contains("EndTest action: Finished. 2 processes terminated.")
    .unwrap();
}
