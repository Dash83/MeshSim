extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::logging::*;

#[test]
fn test_basic() {
    let test = get_test_path("rgr_basic_test.toml");
    let work_dir = create_test_dir("rgr_basic");

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
                                                   "End_Test action: Finished. 30 processes terminated.", 
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    //Check the handshake between the nodes
    let node24_log_file = format!("{}/log/node24.log", &work_dir);
    let node24_log_records = logging::get_log_records_from_file(&node24_log_file).unwrap();

    //node1 receives the command to start transmission
    let node_24_data_recv = logging::find_log_record("msg", "Message 01b903789ae7d54079e92398434cef61 has reached its destination", &node24_log_records);

    assert!(node_24_data_recv.is_some());
}

#[test]
fn test_route_teardown() {
    let test = get_test_path("rgr_route_teardown.toml");
    let work_dir = create_test_dir("rgr_route_teardown");

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
    let _master_node_num = logging::find_log_record("msg",
                                                   "End_Test action: Finished. 23 processes terminated.",
                                                   &master_log_records);

    let node7_log_file = format!("{}/log/node7.log", &work_dir);
    let node7_log_records = logging::get_log_records_from_file(&node7_log_file).unwrap();
    let node7_teardown_recv = logging::find_log_record("msg", "Route TEARDOWN msg received for route 58dd5a265c14d7142b37a481ccd74608", &node7_log_records);


    let mut received_packets = 0;
    let node25_log_file = format!("{}/log/node25.log", &work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();

    for record in node25_log_records.iter() {
        if record["msg"].as_str().unwrap().contains("reached its destination") {
            received_packets += 1;
        }
    }

    //3 packets should arrive before the route is torn-down
    assert_eq!(received_packets, 3);
    //Confirm the route disruption was detected and the source node received it.
    assert!(node7_teardown_recv.is_some());
}

