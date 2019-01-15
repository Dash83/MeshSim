use super::super::*;

#[test]
fn rgr_basic() {
    let test = get_test_path("rgr_basic_test.toml");
    let work_dir = create_test_dir("rgr_basic");

    let program = get_master_path();
    let worker = get_worker_path();


    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&program])
    .with_args(&["-t",  &test, "-w", &worker, "-d", &work_dir])
    .succeeds()
    .and()
    .stdout()
    .contains("End_Test action: Finished. 30 processes terminated.")
    .unwrap();

    //Check the handshake between the nodes
    let node24_log_file = format!("{}/log/node24.log", &work_dir);
    let node24_log_records = logging::get_log_records_from_file(&node24_log_file).unwrap();

    //node1 receives the command to start transmission
    let node_24_data_recv = logging::find_log_record("msg", "Message 90ec6d3eef645f6ac1b82e1966100071 has reached it's destination", &node24_log_records);

    assert!(node_24_data_recv.is_some());
}