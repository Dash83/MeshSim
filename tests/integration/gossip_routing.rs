extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::logging::*;

#[test]
fn gossip_basic() {
    let test = get_test_path("gossip_basic.toml");
    let work_dir = create_test_dir("gossip_basic");

    let program = get_master_path();
    let worker = get_worker_path();


    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&program])
        .with_args(&["-t",  &test, "-w", &worker, "-d", &work_dir])
        .succeeds()
        .unwrap();

    //Check the test ended with the correct number of processes.
    let master_log_file = format!("{}{}{}{}{}", &work_dir,
                                  std::path::MAIN_SEPARATOR,
                                  LOG_DIR_NAME,
                                  std::path::MAIN_SEPARATOR,
                                  DEFAULT_MASTER_LOG);
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
    let master_node_num = logging::find_record_by_msg(
                                                   "End_Test action: Finished. 25 processes terminated.",
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    let node25_log_file = format!("{}/log/node25.log", &work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();
    let node_25_msg_recv = logging::find_record_by_msg(
        "Received DATA message fbedcdf252b06a96e4b6b7323be7d788 from node20",
        &node25_log_records
    );
    assert!(node_25_msg_recv.is_some() && 
            node_25_msg_recv.cloned().unwrap().status.unwrap() == "ACCEPTED");

    //Test passed. Results are not needed.
    std::fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}

#[test]
#[ignore]
fn test_route_discovery_optimization() {
    let test = get_test_path("gossip_route_discovery.toml");
    let work_dir = create_test_dir("gossip_route_discovery");

    let program = get_master_path();
    let worker = get_worker_path();


    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&program])
        .with_args(&["-t",  &test, "-w", &worker, "-d", &work_dir])
        .succeeds()
        .unwrap();

    //Check the test ended with the correct number of processes.
    let master_log_file = format!("{}{}{}{}{}", &work_dir,
                                  std::path::MAIN_SEPARATOR,
                                  LOG_DIR_NAME,
                                  std::path::MAIN_SEPARATOR,
                                  DEFAULT_MASTER_LOG);
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
    let master_node_num = logging::find_record_by_msg(
                                                   "End_Test action: Finished. 100 processes terminated.",
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    //Node 43 should received 16 packets
    let node43_log_file = format!("{}/log/node43.log", &work_dir);
    let node43_log_records = logging::get_log_records_from_file(&node43_log_file).unwrap();
    let mut received_packets = 0;
    for record in node43_log_records.iter() {
        if let Some(status) = &record.status {
            if status == "ACCEPTED" {
                received_packets += 1;
            }
        }
    }
    assert_eq!(received_packets, 16);

    //Node 38 should received 16 packets
    let node38_log_file = format!("{}/log/node38.log", &work_dir);
    let node38_log_records = logging::get_log_records_from_file(&node38_log_file).unwrap();
    let mut received_packets = 0;
    for record in node38_log_records.iter() {
        if let Some(status) = &record.status {
            if status == "ACCEPTED" {
                received_packets += 1;
            }
        }
    }
    assert_eq!(received_packets, 16);

    //Node 45 should received 16 packets
    let node45_log_file = format!("{}/log/node45.log", &work_dir);
    let node45_log_records = logging::get_log_records_from_file(&node45_log_file).unwrap();
    let mut received_packets = 0;
    for record in node45_log_records.iter() {
        if let Some(status) = &record.status {
            if status == "ACCEPTED" {
                received_packets += 1;
            }
        }
    }
    assert_eq!(received_packets, 16);

    //Test passed. Results are not needed.
//    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}