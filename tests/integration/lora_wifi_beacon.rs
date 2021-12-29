extern crate mesh_simulator;

use std::collections::HashMap;
use std::path::Path;

use super::super::*;

use mesh_simulator::tests::common::*;

#[test]
fn test_placement() {
    color_backtrace::install();

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
        "End_Test action: Finished. 12 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    //Load all the logs for all nodes
    let mut all_incoming_messages = HashMap::new();
    let mut all_outgoing_messages = HashMap::new();
    for i in 1..21 {
        let node = format!("node{}", i);
        let log_file = format!("{}/log/{}.log", &data.work_dir, &node);
        let path = Path::new(&log_file);
        if !path.exists() {
            continue;
        }
        let incoming = logging::get_received_message_records(&log_file).expect("Could not get outgoing messages for node");
        let outgoing = logging::get_outgoing_message_records(&log_file).expect("Could not get outgoing messages for node");
        all_incoming_messages.insert(node.clone(), incoming);
        all_outgoing_messages.insert(node.clone(), outgoing);
    }

    //////////////////////////////////////////
    //Check the upper left corner of the grid
    /////////////////////////////////////////
    let node1_beacons_sent_short = all_outgoing_messages["node1"]
        .iter()
        .filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT")
        .count();
    let node1_beacons_sent_long = all_outgoing_messages["node1"]
        .iter()
        .filter(|&m| m.msg_type == "BEACON" && m.radio == "LONG")
        .count();
    let node1_beacons_received = all_incoming_messages["node1"]
        .iter()
        .filter(|&m| m.msg_type == "BEACON" && m.status == "ACCEPTED")
        .count();
    let node1_responses_received = all_incoming_messages["node1"]
        .iter()
        .filter(|&m| m.msg_type == "BEACON_RESPONSE" && m.status == "ACCEPTED")
        .count();

    // Expected beacons & responses per node:
    // node2:   10
    // node3:   5
    // node6:   10
    // node7:   5
    // node11:  5
    let node1_beacons_expected =
        all_outgoing_messages["node2"].iter().filter(|&m| m.msg_type == "BEACON").count() +
        all_outgoing_messages["node6"].iter().filter(|&m| m.msg_type == "BEACON").count() +
        all_outgoing_messages["node3"].iter().filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT").count() +
        all_outgoing_messages["node7"].iter().filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT").count() +
        all_outgoing_messages["node11"].iter().filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT").count();
    assert_eq!(node1_beacons_received, node1_beacons_expected);

    let expected_responses = (node1_beacons_sent_short * 5) + (node1_beacons_sent_long * 2);
    assert_eq!(node1_responses_received, expected_responses);

    ////////////////////////
    //Check a central node
    ////////////////////////
    let node7_beacons_received = all_incoming_messages["node7"]
        .iter()
        .filter(|&m| m.msg_type == "BEACON" && m.status == "ACCEPTED")
        .count();
    let node7_responses_received = all_incoming_messages["node7"]
        .iter()
        .filter(|&m| m.msg_type == "BEACON_RESPONSE" && m.status == "ACCEPTED")
        .count();
    let node7_beacons_sent_short = all_outgoing_messages["node7"]
        .iter()
        .filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT")
        .count();
    let node7_beacons_sent_long = all_outgoing_messages["node7"]
        .iter()
        .filter(|&m| m.msg_type == "BEACON" && m.radio == "LONG")
        .count();

    // Expected beacons & responses per node:
    // node1:   5
    // node3:   5
    // node9:   5
    // node11:  5
    // node13:  5
    // node17:  5 <-- este no
    // node2:  10
    // node6:  10
    // node8:  10
    // node12: 10
    let node7_beacons_expected =
        all_outgoing_messages["node1"].iter().filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT").count() +    
        all_outgoing_messages["node3"].iter().filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT").count() +
        all_outgoing_messages["node9"].iter().filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT").count() +
        all_outgoing_messages["node11"].iter().filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT").count() +
        all_outgoing_messages["node13"].iter().filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT").count() +
        // all_outgoing_messages["node17"].iter().filter(|&m| m.msg_type == "BEACON" && m.radio == "SHORT").count() +
        all_outgoing_messages["node2"].iter().filter(|&m| m.msg_type == "BEACON").count() +
        all_outgoing_messages["node6"].iter().filter(|&m| m.msg_type == "BEACON").count() +
        all_outgoing_messages["node8"].iter().filter(|&m| m.msg_type == "BEACON").count() +
        all_outgoing_messages["node12"].iter().filter(|&m| m.msg_type == "BEACON").count();
    assert_eq!(node7_beacons_received, node7_beacons_expected);
    
    let expected_responses = (node7_beacons_sent_short * 9) + (node7_beacons_sent_long * 4);
    assert_eq!(node7_responses_received, expected_responses);

    //Test passed. Results are not needed.
    teardown(data, true);
}
