//! This module will test serve as the basic sanity tests for device mode, to make sure the platform is working.
//! This module does not test any of the protocols on device mode. While these are basic, almost unit tests, they are kept
//! in the integration tests module since they can't be isolated to function-level and require the interaction of several modules and binaries.
//! 
//! Since this test module addresses device mode in particular, we can't use the master to drive the execution of the tests, and therefore can't 
//! rely on the test_specification features.

//extern crate mesh_simulator;

use super::super::*;
use self::mesh_simulator::worker::*;
use std;
use std::process::Command;
use std::thread;
use std::time::Duration;
use std::io::Read;
use std::process::Stdio;
use std::fs::File;

/// This test makes sure that the basic components of device mode are activated when running the worker on device mode.
/// At the moment, this test is tied up to my lab computer due to network-interface names. Not sure this will be addressed later.
/// This test could be a part of the tests that only run every now and then.
#[test]
#[cfg(target_os = "linux")]
fn integration_device_mode_basic() -> TestResult {
    let host = env::var("MESHSIM_HOST").unwrap_or(String::from(""));
    let test_duration :u64 = 3000; //3000 ms
    let program = get_worker_path();
    let work_dir = create_test_dir("device_mode_basic");
    let log_path = format!("{}/log/worker1.log", &work_dir);

    //This test should ONLY run on my lab development machine due to required configuration of device_mode.
    if !host.eq("kaer-morhen") {
        panic!("This test should only run in the kaer-morhen host");
    }
    
    //Acquire the lock for the NIC since other tests also require it and they conflict with each other. 
    let _nic = WIRELESS_NIC.lock()?;
    
    //Create configuration
    //We only change the configuration properties for the test to run on my dev computer. The rest of the defaults are fine.
    let mut config = worker_config::WorkerConfig::new();
    config.work_dir = work_dir;
    config.operation_mode = OperationMode::Device;
    let mut r_config = worker_config::RadioConfig::new();
    r_config.interface_name = Some(String::from("eno1"));
    config.radio_short = Some(r_config);

    //Write config file
    let config_file = format!("{}{}{}", &config.work_dir, std::path::MAIN_SEPARATOR, "worker1.toml");
    let _res = config.write_to_file(&config_file).expect("Could not write configuration file.");

    println!("Running command: {} -c {}", &program, &config_file);

    let mut child = Command::new(&program)
                        .arg("-c")
                        .arg(&config_file)
                        .stdout(Stdio::piped())
                        .stdin(Stdio::null())
                        .stderr(Stdio::piped())
                        .spawn()
                        .expect("Could not start worker process.");


    //Let the test run and allocate the resources and then terminate it.
    thread::sleep(Duration::from_millis(test_duration));

    //End the process so that we can collect the results    
    child.kill().expect("Could not terminate worker process");
    let _exit = child.wait().expect("Could not get the worker's output for evaluation.");

    let mut log_file = File::open(log_path).ok().expect("Failed to open log file.");
    let mut output = String::new();
    log_file.read_to_string(&mut output).ok().expect("Failed to read log file.");

    println!("Process output: {}", &output);
    assert!(output.contains("Worker finished initializing."));
    assert!(output.contains("Radio initialized."));
    assert!(output.contains("Starting the heartbeat thread."));
    Ok(())
}