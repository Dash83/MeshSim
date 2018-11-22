//! Tests in this module are meant to excercise the code paths that cover the basic functionality of the 
//! tMembership protocol. The initial set of tests act more like unit-tests of the protocol as they cover the
//! basic functionality of it (startup handshake, heartbeat handshake) as well ast the test_action features.
//! 
//! Future tests might cover the protocol in more detail and corner cases.

use super::super::*;

#[test]
fn integration_tmembership_join() {
    let test = get_test_path("join_test.toml");
    let program = get_master_path();
    let worker = get_worker_path();
    let work_dir = create_test_dir("basic_test");

    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&program])
    .with_args(&["-t",  &test, "-w", &worker, "-d", &work_dir])
    .succeeds()
    .and()
    .stdout()
    .contains("End_Test action: Finished. 2 processes terminated.")
    .unwrap();

    //Check the handshake between the nodes
    let node1_log_file = format!("{}/log/node1.log", &work_dir);
    let node1_log_records = logging::get_log_records_from_file(&node1_log_file).unwrap();
    let node2_log_file = &format!("{}/log/node2.log", &work_dir);
    let node2_log_records = logging::get_log_records_from_file(&node2_log_file).unwrap();

    //let node_2_discovery = logging::find_log_record("msg", "Found 1 peers!", &node2_log_records);   
    let node_1_rec_join = logging::find_log_record("msg", "Received JOIN message from node2", &node1_log_records);
    let node_2_ack = logging::find_log_record("msg", "Received ACK message from node1", &node2_log_records);


    //assert!(node_2_discovery.is_some());
    assert!(node_1_rec_join.is_some());
    assert!(node_2_ack.is_some());
}

#[test]
fn heartbeat_test() {
    let test = get_test_path("heartbeat_test.toml");
    let program = get_master_path();
    let worker = get_worker_path();
    let work_dir = create_test_dir("heartbeat_test");

    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&program])
    .with_args(&["-t",  &test, "-w", &worker, "-d", &work_dir])
    .succeeds()
    .and()
    .stdout()
    .contains("End_Test action: Finished. 2 processes terminated.")
    .unwrap();

    //Check the handshake between the nodes
    let node1_log_file = format!("{}/log/node1.log", &work_dir);
    let node1_log_records = logging::get_log_records_from_file(&node1_log_file).unwrap();
    let node2_log_file = &format!("{}/log/node2.log", &work_dir);
    let node2_log_records = logging::get_log_records_from_file(&node2_log_file).unwrap();

    //let node_2_discovery = logging::find_log_record("msg", "Found 1 peers!", &node2_log_records);   
    let node_1_alive = logging::find_log_record("msg", "Received Alive message from node2.", &node1_log_records);
    let node_2_alive = logging::find_log_record("msg", "Received Alive message from node1.", &node2_log_records);


    //assert!(node_2_discovery.is_some());
    assert!(node_1_alive.is_some());
    assert!(node_2_alive.is_some());
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
    .and()
    .stdout()
    .contains("End_Test action: Finished. 2 processes terminated.")
    .unwrap();
}

#[ignore]
#[test]
fn sustained_test() {
    use std;
    
    let test = get_test_path("tmembership_sustained.toml");
    let program = get_master_path();
    let worker = get_worker_path();
    let work_dir = create_test_dir("tmembership_sustained");
    let log_dir = format!("{}{}{}", &work_dir, std::path::MAIN_SEPARATOR, "log");

    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    //Assert the test finished succesfully.
    //The only pass condition for this test is that the specified process is killed during the test execution,
    //therefore leaving only 2 process to be terminated at the end of the test.
    assert_cli::Assert::command(&[&program])
    .with_args(&["-t",  &test, "-w", &worker, "-d", &work_dir])
    .succeeds()
    .and()
    .stdout()
    .contains("End_Test action: Finished. 14 processes terminated.")
    .unwrap();


}
//**************************************//
//******** TMembership advanced ********//
//**************************************//

