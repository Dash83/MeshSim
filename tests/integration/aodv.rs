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
    let node25_log_file = format!("{}/log/node25.log", &work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();

    let mut received_packets = 0 ;
    for record in node25_log_records.iter() {
        if record["msg"].as_str().unwrap().contains("DATA message reached its destination") {
            received_packets += 1;
        }
    }
    assert_eq!(received_packets, 2);

    //Test passed. Results are not needed.
    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}
