extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::tests::common::*;

#[test]
fn test_basic() {
    let test_name = String::from("rgr_basic");
    let data = setup(&test_name, false, false);

    println!(
        "Running command: {} -t {} -w {} -d {}",
        &data.master, &data.test_file, &data.worker, &data.work_dir
    );

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&data.master])
        .with_args(&[
            "-t",
            &data.test_file,
            "-w",
            &data.worker,
            "-d",
            &data.work_dir,
        ])
        .succeeds()
        .unwrap();

    //Check the test ended with the correct number of processes.
    let master_log_file = format!(
        "{}{}{}{}{}",
        &data.work_dir,
        std::path::MAIN_SEPARATOR,
        LOG_DIR_NAME,
        std::path::MAIN_SEPARATOR,
        DEFAULT_MASTER_LOG
    );
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
    let master_node_num = logging::find_record_by_msg(
        "End_Test action: Finished. 25 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    let node25_log_file = format!("{}/log/node25.log", &data.work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();

    let received_packets = count_data_packets(&node25_log_records);
    assert_eq!(received_packets, 1);

    //Test passed. Results are not needed.
    teardown(data, true);
}

#[test]
fn test_route_retry() {
    let test_name = String::from("rgr_route_discovery_retry");
    let data = setup(&test_name, false, false);

    println!(
        "Running command: {} -t {} -w {} -d {}",
        &data.master, &data.test_file, &data.worker, &data.work_dir
    );

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&data.master])
        .with_args(&[
            "-t",
            &data.test_file,
            "-w",
            &data.worker,
            "-d",
            &data.work_dir,
        ])
        .succeeds()
        .unwrap();

    //Check the test ended with the correct number of processes.
    let master_log_file = format!(
        "{}{}{}{}{}",
        &data.work_dir,
        std::path::MAIN_SEPARATOR,
        LOG_DIR_NAME,
        std::path::MAIN_SEPARATOR,
        DEFAULT_MASTER_LOG
    );
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
    let master_node_num = logging::find_record_by_msg(
        "End_Test action: Finished. 6 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    let node2_log_file = format!("{}/log/node2.log", &data.work_dir);
    let node2_log_records = logging::get_log_records_from_file(&node2_log_file).unwrap();
    let node_2_route_retry =
        logging::find_record_by_msg("Re-trying ROUTE_DISCOVERY for node6", &node2_log_records);
    assert!(node_2_route_retry.is_some());

    let node6_log_file = format!("{}/log/node6.log", &data.work_dir);
    let node6_log_records = logging::get_log_records_from_file(&node6_log_file).unwrap();
    let received_packets = count_data_packets(&node6_log_records);
    assert_eq!(received_packets, 1);

    //Test passed. Results are not needed.
    teardown(data, true);
}

#[test]
fn test_route_teardown() {
    let test_name = String::from("rgr_route_teardown");
    let data = setup(&test_name, false, false);

    println!(
        "Running command: {} -t {} -w {} -d {}",
        &data.master, &data.test_file, &data.worker, &data.work_dir
    );

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&data.master])
        .with_args(&[
            "-t",
            &data.test_file,
            "-w",
            &data.worker,
            "-d",
            &data.work_dir,
        ])
        .succeeds()
        .unwrap();

    //Check the test ended with the correct number of processes.
    let master_log_file = format!(
        "{}{}{}{}{}",
        &data.work_dir,
        std::path::MAIN_SEPARATOR,
        LOG_DIR_NAME,
        std::path::MAIN_SEPARATOR,
        DEFAULT_MASTER_LOG
    );
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
    let _master_node_num = logging::find_record_by_msg(
        "End_Test action: Finished. 23 processes terminated.",
        &master_log_records,
    );

    let node7_log_file = format!("{}/log/node7.log", &data.work_dir);
    let _node7_log_records = logging::get_log_records_from_file(&node7_log_file).unwrap();
    let node7_incoming =
        get_received_message_records(node7_log_file).expect("Could not read incoming packets");
    let node7_teardown_recv = node7_incoming
        .iter()
        .filter(|&m| m.msg_type == "ROUTE_TEARDOWN" && m.status == "FORWARDING")
        .count();

    let node25_log_file = format!("{}/log/node25.log", &data.work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();
    let received_packets = count_data_packets(&node25_log_records);

    //3 packets should arrive before the route is torn-down
    assert_eq!(received_packets, 3);
    //Confirm the route disruption was detected and the source node received it.
    assert_eq!(node7_teardown_recv, 1);

    //Test passed. Results are not needed.
    teardown(data, true);
}

#[test]
fn test_route_discovery() {
    let test_name = String::from("rgrI_route_discovery_optimization");
    let data = setup(&test_name, false, false);

    println!(
        "Running command: {} -t {} -w {} -d {}",
        &data.master, &data.test_file, &data.worker, &data.work_dir
    );

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&data.master])
        .with_args(&[
            "-t",
            &data.test_file,
            "-w",
            &data.worker,
            "-d",
            &data.work_dir,
        ])
        .succeeds()
        .unwrap();

    //Check the test ended with the correct number of processes.
    let master_log_file = format!(
        "{}{}{}{}{}",
        &data.work_dir,
        std::path::MAIN_SEPARATOR,
        LOG_DIR_NAME,
        std::path::MAIN_SEPARATOR,
        DEFAULT_MASTER_LOG
    );
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
    let master_node_num = logging::find_record_by_msg(
        "End_Test action: Finished. 81 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    //Node 43 should received 16 packets
    let node43_log_file = format!("{}/log/node43.log", &data.work_dir);
    let node43_log_records = logging::get_log_records_from_file(&node43_log_file).unwrap();
    let received_packets = count_data_packets(&node43_log_records);
    assert_eq!(received_packets, 16);

    //Node 38 should received 16 packets
    let node38_log_file = format!("{}/log/node38.log", &data.work_dir);
    let node38_log_records = logging::get_log_records_from_file(&node38_log_file).unwrap();
    let received_packets = count_data_packets(&node38_log_records);
    assert_eq!(received_packets, 16);

    //Node 45 should received 16 packets
    let node45_log_file = format!("{}/log/node45.log", &data.work_dir);
    let node45_log_records = logging::get_log_records_from_file(&node45_log_file).unwrap();
    let received_packets = count_data_packets(&node45_log_records);
    assert_eq!(received_packets, 16);

    //Test passed. Results are not needed.
    teardown(data, true);
}

#[ignore]
#[test]
fn test_packet_counting() {
    let path = String::from("/tmp/rgrII_route_discovery_optimization_1575207368/log/");
    let _num = count_all_packets("RouteDiscovery", &path);
}
