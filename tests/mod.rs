extern crate assert_cli;
extern crate chrono;
extern crate mesh_simulator;

#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;
// #[macro_use]
extern crate lazy_static;
// #[macro_use]
extern crate slog;
#[macro_use]
extern crate peroxide;

use self::chrono::prelude::*;
use self::mesh_simulator::logging::{self, *};
use std::env;

use std::process::{Command, Stdio};

// mod experiments;
// pub mod common;
mod integration;
mod unit;

/***********************************************/
/**************** Helper functions *************/
/***********************************************/

// //MESHIM_TEST_DIR should point to "/media/marco/Data/Tests/" at my lab computer.
// fn get_tests_root() -> String {
//     let test_home = env::var("MESHSIM_TEST_DIR").unwrap_or(String::from("/tmp/"));
//     test_home
// }

// fn create_integration_test_dir<'a>(test_name : &'a str) -> String {
//     let now : DateTime<Utc> = Utc::now();
//     let test_dir_path = format!("{}{}_{}", &get_tests_root(),
//                                              test_name,
//                                              now.timestamp());
//     let test_dir = Path::new(&test_dir_path);

//     if !test_dir.exists() {
//         fs::create_dir(&test_dir_path).expect(&format!("Unable to create test results directory {}", test_dir_path));
//     }

//     test_dir_path.clone()
// }
