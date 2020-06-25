extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::tests::common::*;

#[test]
fn aodv_basic() {
    let test_name = String::from("aodv_basic");
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

    let mut received_packets = 0;
    for record in node25_log_records.iter() {
        if record.status.is_some() && record.msg_type.is_some() {
            let status = &record.status.clone().unwrap();
            let msg_type = &record.msg_type.clone().unwrap();
            if status == "ACCEPTED" && msg_type == "DATA" {
                received_packets += 1;
            }
        }
    }
    assert_eq!(received_packets, 2);

    //Test passed. Results are not needed.
    teardown(data, true);
}

/// This test is designed to evaluate the route error detecting capabilities of AODV.
/// It consists of 25 nodes arranged in a 5x5 grid.
/// At some point, Node1 attempts to sent a data packet to Node5. The shortest path
/// for this packet would be Node1->Node2->Node3->Node4->Node5. AODV will attempt to
/// calculate and use this route.
///
/// After the data packet is successfully delivered, nodes 4 and 9 will be killed.
/// Those two nodes are adjecent to Node5 and will leave it disconnected from the
/// rest of the network.
///
/// The following things must be tested afterwards:
/// 1. That Node5 detects the severed links, and broadcasts an RERR message (although no node will hear it).
/// 2. That nodes adjecent to Node4 and Node9 detect the broken links as well and generate RERR messages of their own.
#[test]
fn aodv_rerr() {
    let test_name = String::from("aodv_rerr");
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
        "End_Test action: Finished. 23 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    // Evaluate the behaviour of Node5
    // Node5 must receive 1 data packet.
    // Node5 must send out an RERR message in response to the broken links
    let node5_log_file = format!("{}/log/node5.log", &data.work_dir);
    let node5_log_records = logging::get_log_records_from_file(&node5_log_file).unwrap();
    let outgoing_messages = logging::get_outgoing_message_records(node5_log_file).unwrap();
    let data_packets = count_data_packets(&node5_log_records);
    let rerr_msgs_sent = outgoing_messages
        .iter()
        .filter(|&m| m.msg_type == "RERR" && m.status == "SENT")
        .count();

    assert_eq!(data_packets, 1);
    assert_eq!(rerr_msgs_sent, 1);

    //Evaluate the behaviour of Node3
    // Node3 is part of the route that was established between Node1 and Node5 and thus
    // Should detect very quickly that Node4 is down, and react by sending an RERR.
    let node3_log_file = format!("{}/log/node3.log", &data.work_dir);
    let outgoing_messages = logging::get_outgoing_message_records(node3_log_file).unwrap();
    let rerr_msgs_sent = outgoing_messages
        .iter()
        .filter(|&m| m.msg_type == "RERR" && m.status == "SENT")
        .count();
    assert!(rerr_msgs_sent > 0);

    //Evaluate the behaviour of Node9
    //Similar to Node3, Node9 is adjacent to one of the nodes that goes down.
    //I'm not actually sure this node should detect Node10 goes down, as no active routes
    //should be present in its route table, but I'm currently assuming it should.
    //TODO: This assumption should be revisited amd if this node consistently sends out its RERR,
    //that might be a symptom of incorrect behaviour that causes unecessary route breaks.
    let node9_log_file = format!("{}/log/node9.log", &data.work_dir);
    let outgoing_messages = logging::get_outgoing_message_records(node9_log_file).unwrap();
    let rerr_msgs_sent = outgoing_messages
        .iter()
        .filter(|&m| m.msg_type == "RERR" && m.status == "SENT")
        .count();
    assert_eq!(rerr_msgs_sent, 1);

    // //Evaluate the behaviour of Node15
    // let node15_log_file = format!("{}/log/node15.log", &data.work_dir);
    // let outgoing_messages = logging::get_outgoing_message_records(node15_log_file).unwrap();
    // let rerr_msgs_sent = outgoing_messages
    //     .iter()
    //     .filter(|&m| m.msg_type == "RERR" && m.status == "SENT")
    //     .collect::<Vec<_>>()
    //     .len();
    // assert_eq!(rerr_msgs_sent, 1);

    // //Test passed. Results are not needed.
    teardown(data, true);
}
