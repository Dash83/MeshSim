extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::logging::*;

#[test]
fn test_placement() {
    let test = get_test_path("lora_wifi_beacon_placement.toml");
    let work_dir = create_test_dir("lora_wifi_beacon_placement");

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
                                                   "End_Test action: Finished. 20 processes terminated.",
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    //Check the handshake between the nodes
    let node1_log_file = format!("{}/log/node1.log", &work_dir);
    let node1_log_records = logging::get_log_records_from_file(&node1_log_file).unwrap();
    let mut received_packets_wifi = 0;
    let mut received_packets_lora = 0;
    for record in node1_log_records.iter() {
        if record["msg"].as_str().unwrap().contains("Beacon received over wifi from") {
            received_packets_wifi += 1;
        }

        if record["msg"].as_str().unwrap().contains("Beacon received over lora from") {
            received_packets_lora += 1;
        }
    }

    assert_eq!(received_packets_wifi, 20);
    assert_eq!(received_packets_lora, 20);
}