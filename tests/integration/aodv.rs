extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::logging::*;

#[test]
fn aodv_basic() {
    let test = get_test_path("aodv_basic.toml");
    let work_dir = create_test_dir("aodv_basic");

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
    let master_node_num = logging::find_log_record("msg", 
                                                   "End_Test action: Finished. 25 processes terminated.", 
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    //Check the handshake between the nodes
    // let node1_log_file = format!("{}/log/node1.log", &work_dir);
    // let node1_log_records = logging::get_log_records_from_file(&node1_log_file).unwrap();
    // let node2_log_file = &format!("{}/log/node2.log", &work_dir);
    // let node2_log_records = logging::get_log_records_from_file(&node2_log_file).unwrap();
    // let node3_log_file = &format!("{}/log/node3.log", &work_dir);
    // let node3_log_records = logging::get_log_records_from_file(&node3_log_file).unwrap();

    // //node1 receives the command to start transmission
    // let node_1_cmd_recv = logging::find_log_record("msg", "Send command received", &node1_log_records);
    // //node1 sends the message. node2 is the only node in range.
    // let node_1_msg_sent = logging::find_log_record("msg", "Message 763ecd437c5bd4aa764380b63d5951ba sent", &node1_log_records);
    // //node2 receives the message. It's a new message so it relays it
    // let node_2_msg_recv = logging::find_log_record("msg", "Received DATA message 763ecd437c5bd4aa764380b63d5951ba from node1", &node2_log_records);   
    // //node3 receives the message. Since node3 it's the intended receiver, it does not relay it
    // let node_3_msg_recv = logging::find_log_record("msg", "Message 763ecd437c5bd4aa764380b63d5951ba reached its destination", &node3_log_records);   
    // //node1 also receives the message from node2. Since it has never received the message from that node, it relays it for reliability.
    // let node_1_msg_recv = logging::find_log_record("msg", "Received DATA message 763ecd437c5bd4aa764380b63d5951ba from node2", &node1_log_records);

    // assert!(node_1_cmd_recv.is_some());
    // assert!(node_1_msg_sent.is_some());
    // assert!(node_2_msg_recv.is_some());
    // assert!(node_3_msg_recv.is_some());
    // assert!(node_1_msg_recv.is_some());

    // //Test passed. Results are not needed.
    // fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
    panic!("Inspect logs!");
}
