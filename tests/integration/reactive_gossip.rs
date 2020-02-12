extern crate mesh_simulator;

use super::super::*;

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
                                                   "End_Test action: Finished. 25 processes terminated.", 
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    let node25_log_file = format!("{}/log/node25.log", &work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();

    let received_packets = count_data_packets(&node25_log_records);
    assert_eq!(received_packets, 1);

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
        "Re-trying ROUTE_DISCOVERY for node6", 
        &node2_log_records
    );
    assert!(node_2_route_retry.is_some());

    let node6_log_file = format!("{}/log/node6.log", &work_dir);
    let node6_log_records = logging::get_log_records_from_file(&node6_log_file).unwrap();
    let received_packets = count_data_packets(&node6_log_records);
    assert_eq!(received_packets, 1);

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
    // let node7_teardown_recv = logging::find_record_by_msg(
    //     "Route TEARDOWN msg received for route ec3a7a8fc1f4a1c13c933cadb5f880e8", 
    //     &node7_log_records
    // );
    let mut node7_teardown_recv = false;
    for record in node7_log_records.iter() {
        if record.msg != "Received ROUTE_TEARDOWN message" {
            continue;
        }

        if let Some(status) = &record.status {
            node7_teardown_recv = status == "FORWARDING";
        }
    }

    let node25_log_file = format!("{}/log/node25.log", &work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();
    let received_packets = count_data_packets(&node25_log_records);

    //3 packets should arrive before the route is torn-down
    assert_eq!(received_packets, 3);
    //Confirm the route disruption was detected and the source node received it.
    assert!(node7_teardown_recv);

    //Test passed. Results are not needed.
    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}

#[test]
fn test_route_discovery() {
    let test = get_test_path("rgrI_route_discovery_optimization.toml");
    let work_dir = create_test_dir("rgrI_route_discovery_optimization");

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
                                                   "End_Test action: Finished. 81 processes terminated.",
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    //Node 43 should received 16 packets
    let node43_log_file = format!("{}/log/node43.log", &work_dir);
    let node43_log_records = logging::get_log_records_from_file(&node43_log_file).unwrap();
    let received_packets = count_data_packets(&node43_log_records);
    assert_eq!(received_packets, 16);

    //Node 38 should received 16 packets
    let node38_log_file = format!("{}/log/node38.log", &work_dir);
    let node38_log_records = logging::get_log_records_from_file(&node38_log_file).unwrap();
    let received_packets = count_data_packets(&node38_log_records);
    assert_eq!(received_packets, 16);

    //Node 45 should received 16 packets
    let node45_log_file = format!("{}/log/node45.log", &work_dir);
    let node45_log_records = logging::get_log_records_from_file(&node45_log_file).unwrap();
    let received_packets = count_data_packets(&node45_log_records);
    assert_eq!(received_packets, 16);

    //Test passed. Results are not needed.
   fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}

#[ignore]
#[test]
fn test_packet_counting() {
    let path = String::from("/tmp/rgrII_route_discovery_optimization_1575207368/log/");
    let num = count_all_packets("RouteDiscovery", &path);

}