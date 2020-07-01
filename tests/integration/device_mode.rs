//! This module will test serve as the basic sanity tests for device mode, to make sure the platform is working.
//! This module does not test any of the protocols on device mode. While these are basic, almost unit tests, they are kept
//! in the integration tests module since they can't be isolated to function-level and require the interaction of several modules and binaries.
//!
//! Since this test module addresses device mode in particular, we can't use the master to drive the execution of the tests, and therefore can't
//! rely on the test_specification features.

extern crate mesh_simulator;
use super::super::*;
use mesh_simulator::tests::common::*;
use mesh_simulator::worker::worker_config;
use mesh_simulator::worker::OperationMode;

use std::thread;
use std::time::Duration;

/// This test makes sure that the basic components of device mode are activated when running the worker on device mode.
/// At the moment, this test is tied up to my lab computer due to network-interface names. Not sure this will be addressed later.
/// This test could be a part of the tests that only run every now and then.
#[test]
// #[cfg(target_os = "linux")]
fn integration_device_mode_basic() -> TestResult {
    let host = env::var("MESHSIM_HOST").unwrap_or(String::from(""));
    let test_duration: u64 = 3000; //3000 ms
    let test_name = String::from("device_mode_basic");
    let data = setup(&test_name, false, false);

    //This test should ONLY run on my lab development machine due to required configuration of device_mode.
    if !host.eq("kaer-morhen") {
        println!("This test should only run in the kaer-morhen host");
        return Ok(());
    }

    //Acquire the lock for the NIC since other tests also require it and they conflict with each other.
    let _nic = WIRELESS_NIC.lock()?;

    //Create configuration
    //We only change the configuration properties for the test to run on my dev computer. The rest of the defaults are fine.
    let mut config = worker_config::WorkerConfig::new();
    config.work_dir = data.work_dir.clone();
    config.operation_mode = OperationMode::Device;
    let mut r_config = worker_config::RadioConfig::new();
    r_config.interface_name = Some(String::from("eno1"));
    config.radio_short = Some(r_config);

    //Write config file
    let config_file = format!(
        "{}{}{}",
        &config.work_dir,
        std::path::MAIN_SEPARATOR,
        "worker1.toml"
    );
    let _res = config
        .write_to_file(&config_file)
        .expect("Could not write configuration file.");

    println!("Running command: {} -c {}", &data.worker, &config_file);

    let mut child = Command::new(&data.worker)
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
    let _exit = child
        .wait()
        .expect("Could not get the worker's output for evaluation.");

    // let mut log_file = File::open(&data.work_dir).ok().expect("Failed to open log file.");
    // let mut output = String::new();
    // log_file.read_to_string(&mut output).ok().expect("Failed to read log file.");
    let worker1_log_file = &format!("{}/log/worker1.log", &data.work_dir);
    let _worker1_log_records = logging::get_log_records_from_file(&worker1_log_file).unwrap();

    //TODO: Review that this is the right criteria for an integration test of the device mode
    // println!("Process output: {}", &output);
    // assert!(output.contains("Worker finished initializing."));
    // assert!(output.contains("Radio initialized."));
    // //assert!(output.contains("Starting the heartbeat thread."));
    // for record in worker1_log_records.iter() {
    //     if let Some(status) = &record.status {
    //         if status == "ACCEPTED" {
    //             received_packets += 1;
    //         }
    //     }
    // }
    //Test passed. Results are not needed.
    teardown(data, false);

    Ok(())
}
