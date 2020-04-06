extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::logging::*;
use mesh_simulator::tests::common::*;

#[test]
fn test_random_waypoint_basic() {
    let test_name = String::from("random_waypoint");
    let data= setup(&test_name, false, false);

    println!("Running command: {} -t {} -w {} -d {}", &data.master, &data.test_file, &data.worker, &data.work_dir);

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&data.master])
    .with_args(&["-t",  &data.test_file, "-w", &data.worker, "-d", &data.work_dir])
    .succeeds()
    .unwrap();

    //Check the test ended with the correct number of processes.
    let master_log_file = format!("{}{}{}{}{}", &data.work_dir,
                                                std::path::MAIN_SEPARATOR,
                                                LOG_DIR_NAME,
                                                std::path::MAIN_SEPARATOR,
                                                DEFAULT_MASTER_LOG);
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
    let master_node_num = logging::find_record_by_msg(
                                                   "End_Test action: Finished. 5 processes terminated.", 
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    let node_3_arrived = logging::find_record_by_msg("1 workers have reached their destinations", &master_log_records);
    assert!(node_3_arrived.is_some());

    //Test passed. Results are not needed.
    teardown(data, true);
}
