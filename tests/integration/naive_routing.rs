extern crate mesh_simulator;

use super::super::*;

use mesh_simulator::tests::common::*;

#[test]
fn naive_basic() {
    let test_name = String::from("naive_basic");
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
        "End_Test action: Finished. 3 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    let node1_log_file = format!("{}/log/node1.log", &data.work_dir);
    let node2_log_file = &format!("{}/log/node2.log", &data.work_dir);
    let node3_log_file = &format!("{}/log/node3.log", &data.work_dir);
    let node1_log_records = logging::get_log_records_from_file(&node1_log_file).unwrap();
    let incoming_node1 =
        get_incoming_message_records(&node1_log_file).expect("Could not read incoming packets");
    let outgoing_node1 =
        get_outgoing_message_records(&node1_log_file).expect("Could not read outgoing packets");
    let incoming_node2 =
        get_incoming_message_records(&node2_log_file).expect("Could not read incoming packets");
    let incoming_node3 =
        get_incoming_message_records(&node3_log_file).expect("Could not read incoming packets");

    //node1 receives the command to start transmission
    let node_1_cmd_recv = logging::find_record_by_msg("Send command received", &node1_log_records);
    //node1 sends the message. node2 is the only node in range.
    let node_1_msg_sent = outgoing_node1
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "SENT" && m.source == "node1")
        .count();

    //node2 receives the message. It's a new message so it relays it
    let node_2_msg_recv = incoming_node2
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.source == "node1" && m.status == "FORWARDING")
        .count();

    //node3 receives the message. Since node3 it's the intended receiver, it does not relay it
    let node_3_msg_recv = incoming_node3
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();

    //node1 also receives the message from node2. Since the message originated at node1, it's considered a duplicate and dropped.
    let node_1_msg_recv = incoming_node1
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "DROPPED" && m.reason == "DUPLICATE")
        .count();

    assert!(node_1_cmd_recv.is_some());
    assert_eq!(node_1_msg_sent, 1);
    assert_eq!(node_2_msg_recv, 1);
    assert_eq!(node_3_msg_recv, 1);
    assert_eq!(node_1_msg_recv, 1);

    //Test passed. Results are not needed.
    teardown(data, true);
}

#[test]
fn naive_latency() {
    let test_name = String::from("naive_basic");
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
        "End_Test action: Finished. 3 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    let node1_log_file = format!("{}/log/node1.log", &data.work_dir);
    let node1_log_records = logging::get_log_records_from_file(&node1_log_file).unwrap();

    let (msg_id, ts0) = {
        let record = logging::find_record_by_msg("New message", &node1_log_records)
            .expect("Could not find new message record");
        let msg_id = record.msg_id.clone().expect("No msg_id recorded");
        let ts = record.ts.clone();
        (msg_id, ts)
    };
    println!("msg_id: {}, ts0: {}", &msg_id, &ts0);

    let node3_log_file = &format!("{}/log/node3.log", &data.work_dir);
    let node3_log_records = logging::get_log_records_from_file(&node3_log_file).unwrap();
    let mut node_3_accepted = node3_log_records
        .iter()
        .filter(|&m| m.msg_id == Some(msg_id.clone()) && m.status == Some(String::from("ACCEPTED")))
        .collect::<Vec<&LogEntry>>();
    println!("node3_accepted: {:?}", &node_3_accepted);
    let ts1 = node_3_accepted
        .pop()
        .expect("Did not find corresponding accepted msg")
        .ts
        .clone();
    assert!(ts0 < ts1);
}
#[test]
fn killnode_test() {
    let test_name = String::from("killnode");
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
        "End_Test action: Finished. 2 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    //Test passed. Results are not needed.
    teardown(data, true);
}

#[test]
#[ignore]
fn test_route_discovery_optimization() {
    let test_name = String::from("naive_route_discovery");
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
        "End_Test action: Finished. 100 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    //Node 43 should received 16 packets
    let node43_log_file = format!("{}/log/node43.log", &data.work_dir);
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
    let node38_log_file = format!("{}/log/node38.log", &data.work_dir);
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
    let node45_log_file = format!("{}/log/node45.log", &data.work_dir);
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
    teardown(data, true);
}
