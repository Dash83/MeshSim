use super::super::*;

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
    .and()
    .stdout()
    .contains("End_Test action: Finished. 3 processes terminated.")
    .unwrap();

    //Check the handshake between the nodes
    let node1_log_file = format!("{}/log/node1.log", &work_dir);
    let node1_log_records = logging::get_log_records_from_file(&node1_log_file).unwrap();
    let node2_log_file = &format!("{}/log/node2.log", &work_dir);
    let node2_log_records = logging::get_log_records_from_file(&node2_log_file).unwrap();
    let node3_log_file = &format!("{}/log/node3.log", &work_dir);
    let node3_log_records = logging::get_log_records_from_file(&node3_log_file).unwrap();

    //node1 receives the command to start transmission
    let node_1_cmd_recv = logging::find_log_record("msg", "Command received", &node1_log_records);
    //node1 sends the message. node2 is the only node in range.
    let node_1_msg_sent = logging::find_log_record("msg", "Message 645a15a2fcadd06793b5cdd5137d45e3 sent", &node1_log_records);
    //node2 receives the message. It's a new message so it relays it
    let node_2_msg_recv = logging::find_log_record("msg", "Received DATA message 645a15a2fcadd06793b5cdd5137d45e3 from node1", &node2_log_records);   
    //node3 receives the message. Since node3 it's the intended receiver, it does not relay it
    let node_3_msg_recv = logging::find_log_record("msg", "Message ef6fc096b08373dda66023c3cea363ef reached its destination", &node3_log_records);   
    //node1 also receives the message from node2. Since it has never received the message from that node, it relays it for reliability.
    let node_1_msg_recv = logging::find_log_record("msg", "Received DATA message ef6fc096b08373dda66023c3cea363ef from node2", &node1_log_records);
    //node2 receives the message from node1 again. This time it drops it.
    let node_2_msg_drop = logging::find_log_record("msg", "Dropping repeated message 645a15a2fcadd06793b5cdd5137d45e3", &node2_log_records);   

    assert!(node_1_cmd_recv.is_some());
    assert!(node_1_msg_sent.is_some());
    assert!(node_2_msg_recv.is_some());
    assert!(node_3_msg_recv.is_some());
    assert!(node_1_msg_recv.is_some());
    assert!(node_2_msg_drop.is_some());

}