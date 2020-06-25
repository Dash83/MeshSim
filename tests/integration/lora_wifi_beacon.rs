extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::logging::*;
use mesh_simulator::tests::common::*;

#[test]
fn test_placement() {
    let test_name = String::from("lora_wifi_beacon_placement");
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
    let master_log_records = logging::get_log_records_from_file(&master_log_file)
        .expect("Failed to get log records from Master");
    let master_node_num = logging::find_record_by_msg(
        "End_Test action: Finished. 20 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    //Check the upper left corner of the grid
    let node1_log_file = format!("{}/log/node1.log", &data.work_dir);
    let incoming_messages = logging::get_incoming_message_records(node1_log_file).unwrap();
    let beacons_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "BEACON" && m.status == "ACCEPTED")
        .collect::<Vec<_>>()
        .len();
    let responses_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "BEACON_RESPONSE" && m.status == "ACCEPTED")
        .collect::<Vec<_>>()
        .len();

    println!("beacons_received: {}", beacons_received);
    println!("responses_received: {}", responses_received);
    // Expected beacons per node:
    // node2:   10
    // node3:   5
    // node6:   10
    // node7:   5
    // node11:  5
    assert_eq!(beacons_received, 35);
    // Expected responses per node:
    // node2:   10
    // node3:   5
    // node6:   10
    // node7:   5
    // node11:  5
    assert_eq!(responses_received, 35);

    //Check a central node
    let node7_log_file = format!("{}/log/node7.log", &data.work_dir);
    let incoming_messages = logging::get_incoming_message_records(node7_log_file).unwrap();
    let beacons_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "BEACON" && m.status == "ACCEPTED")
        .collect::<Vec<_>>()
        .len();
    let responses_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "BEACON_RESPONSE" && m.status == "ACCEPTED")
        .collect::<Vec<_>>()
        .len();

    println!("beacons_received: {}", beacons_received);
    println!("responses_received: {}", responses_received);
    assert_eq!(beacons_received, 70);
    assert_eq!(responses_received, 70);

    //Test passed. Results are not needed.
    teardown(data, true);
}
