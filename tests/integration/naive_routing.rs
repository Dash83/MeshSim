use super::super::*;

#[ignore]
#[test]
fn naive_basic() {
    let test = get_test_path("naive_basic_test.toml");
    let program = get_master_path();
    let worker = get_worker_path();
    let work_dir = create_test_dir("naive_basic");

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
    let node2_log_file = &format!("{}/log/node3.log", &work_dir);
    let node2_log_records = logging::get_log_records_from_file(&node2_log_file).unwrap();

    //TODO: This test will fail. Worker commands are needed to broadcast data over the nodes.
    //let node_2_discovery = logging::find_log_record("msg", "Found 1 peers!", &node2_log_records);   
    // let node_1_rec_join = logging::find_log_record("msg", "Received JOIN message from node2", &node1_log_records);
    // let node_2_ack = logging::find_log_record("msg", "Received ACK message from node1", &node2_log_records);


    // //assert!(node_2_discovery.is_some());
    // assert!(node_1_rec_join.is_some());
    // assert!(node_2_ack.is_some());
    assert!(false);
}