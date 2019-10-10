extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::logging::*;

#[test]
fn test_basic() {
    let test = get_test_path("rgr_basic_test.toml");
    let work_dir = create_test_dir("rgr_basic");

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
                                                   "End_Test action: Finished. 30 processes terminated.", 
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    //Check the handshake between the nodes
    let node24_log_file = format!("{}/log/node24.log", &work_dir);
    let node24_log_records = logging::get_log_records_from_file(&node24_log_file).unwrap();

    //node1 receives the command to start transmission
    let node_24_data_recv = logging::find_record_by_msg(
        "Received message 01b903789ae7d54079e92398434cef61", 
        &node24_log_records
    );
    assert!(node_24_data_recv.is_some() && 
            node_24_data_recv.cloned().unwrap().status.unwrap() == "ACCEPTED");

    //Test passed. Results are not needed.
    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}

#[test]
fn test_route_retry() {
    let test = get_test_path("rgr_route_discovery_retry.toml");
    let work_dir = create_test_dir("rgr_route_discovery_retry");

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
                                                   "End_Test action: Finished. 6 processes terminated.", 
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    let node2_log_file = format!("{}/log/node2.log", &work_dir);
    let node2_log_records = logging::get_log_records_from_file(&node2_log_file).unwrap();
    let node_2_route_retry = logging::find_record_by_msg(
        "Re-trying RouteDiscover for node6", 
        &node2_log_records
    );
    assert!(node_2_route_retry.is_some());

    let node6_log_file = format!("{}/log/node6.log", &work_dir);
    let node6_log_records = logging::get_log_records_from_file(&node6_log_file).unwrap();
    let node_6_data_recv = logging::find_record_by_msg(
        "Received message 64d6f18ec2145cdc6cddb19a908e5b2a", 
        &node6_log_records
    );
    assert!(node_6_data_recv.is_some() && 
            node_6_data_recv.cloned().unwrap().status.unwrap() == "ACCEPTED");

    //Test passed. Results are not needed.
    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}

#[test]
fn test_route_teardown() {
    let test = get_test_path("rgr_route_teardown.toml");
    let work_dir = create_test_dir("rgr_route_teardown");

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
    let _master_node_num = logging::find_record_by_msg(
                                                   "End_Test action: Finished. 23 processes terminated.",
                                                   &master_log_records);

    let node7_log_file = format!("{}/log/node7.log", &work_dir);
    let node7_log_records = logging::get_log_records_from_file(&node7_log_file).unwrap();
    let node7_teardown_recv = logging::find_record_by_msg(
        "Route TEARDOWN msg received for route ec3a7a8fc1f4a1c13c933cadb5f880e8", 
        &node7_log_records
    );

    let mut received_packets = 0;
    let node25_log_file = format!("{}/log/node25.log", &work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();

    for record in node25_log_records.iter() {
        if let Some(status) = &record.status {
            if status == "ACCEPTED" {
                received_packets += 1;
            }
        }
    }

    //3 packets should arrive before the route is torn-down
    assert_eq!(received_packets, 3);
    //Confirm the route disruption was detected and the source node received it.
    assert!(node7_teardown_recv.is_some());

    //Test passed. Results are not needed.
    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}

#[test]
#[ignore]
fn test_route_discovery() {
    let test = get_test_path("rgrI_route_discovery.toml");
    let work_dir = create_test_dir("rgrI_route_discovery");

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