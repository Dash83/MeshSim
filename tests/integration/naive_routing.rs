extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::logging::*;

#[test]
fn naive_basic() {
    let test = get_test_path("naive_basic_test.toml");
    let work_dir = create_test_dir("naive_basic");

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
        "End_Test action: Finished. 3 processes terminated.",
        &master_log_records
    );
    assert!(master_node_num.is_some());

    //Check the handshake between the nodes
    let node1_log_file = format!("{}/log/node1.log", &work_dir);
    let node1_log_records = logging::get_log_records_from_file(&node1_log_file).unwrap();
    let node2_log_file = &format!("{}/log/node2.log", &work_dir);
    let node2_log_records = logging::get_log_records_from_file(&node2_log_file).unwrap();
    let node3_log_file = &format!("{}/log/node3.log", &work_dir);
    let node3_log_records = logging::get_log_records_from_file(&node3_log_file).unwrap();

    //node1 receives the command to start transmission
    let node_1_cmd_recv = logging::find_record_by_msg("Send command received", &node1_log_records);
    //node1 sends the message. node2 is the only node in range.
    let node_1_msg_sent = logging::find_record_by_msg("Message 763ecd437c5bd4aa764380b63d5951ba sent", &node1_log_records);
    //node2 receives the message. It's a new message so it relays it
    let node_2_msg_recv = logging::find_record_by_msg("Received message 763ecd437c5bd4aa764380b63d5951ba", &node2_log_records);   
    //node3 receives the message. Since node3 it's the intended receiver, it does not relay it
    let node_3_msg_recv = logging::find_record_by_msg("Received message 763ecd437c5bd4aa764380b63d5951ba", &node3_log_records);   
    //node1 also receives the message from node2. Since it has never received the message from that node, it relays it for reliability.
    let node_1_msg_recv = logging::find_record_by_msg("Received message 763ecd437c5bd4aa764380b63d5951ba", &node1_log_records);

    assert!(node_1_cmd_recv.is_some());
    assert!(node_1_msg_sent.is_some());
    assert!(node_2_msg_recv.is_some());
    assert!(node_3_msg_recv.is_some() && node_3_msg_recv.cloned().unwrap().status.unwrap() == "ACCEPTED");
    assert!(node_1_msg_recv.is_some());

    //Test passed. Results are not needed.
    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}

#[test]
fn killnode_test() {
    let test = get_test_path("killnode_test.toml");
    let program = get_master_path();
    let worker = get_worker_path();
    let work_dir = create_test_dir("killnode_test");

    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    //Assert the test finished succesfully.
    //The only pass condition for this test is that the specified process is killed during the test execution,
    //therefore leaving only 2 process to be terminated at the end of the test.
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
        "End_Test action: Finished. 2 processes terminated.",
        &master_log_records
    );
    assert!(master_node_num.is_some());

    //Test passed. Results are not needed.
    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}

#[test]
#[ignore]
fn test_route_discovery_optimization() {
    let test = get_test_path("naive_route_discovery.toml");
    let work_dir = create_test_dir("naive_route_discovery");

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
        &master_log_records
    );
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