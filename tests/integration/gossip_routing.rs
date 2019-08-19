extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::logging::*;

#[test]
fn gossip_basic() {
    let test = get_test_path("gossip_basic.toml");
    let work_dir = create_test_dir("gossip_basic");

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

    let node25_log_file = format!("{}/log/node25.log", &work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();
    let node_25_msg_recv = logging::find_log_record(
        "msg",
        "Message fbedcdf252b06a96e4b6b7323be7d788 reached its destination",
        &node25_log_records
    );
    assert!(node_25_msg_recv.is_some());

    //Test passed. Results are not needed.
    std::fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}