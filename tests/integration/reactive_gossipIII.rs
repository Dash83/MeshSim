extern crate mesh_simulator;

use super::super::*;

use mesh_simulator::tests::common::*;

fn count_data_packets(log_recors: &Vec<LogEntry>) -> usize {
    let mut packet_count = 0;
    for record in log_recors.iter() {
        if record.status.is_some() && record.msg_type.is_some() {
            let status = &record.status.clone().unwrap();
            let msg_type = &record.msg_type.clone().unwrap();
            if status == "ACCEPTED" && msg_type == "DATA" {
                packet_count += 1;
            }
        }
    }
    packet_count
}

#[test]
fn test_basic() {
    let test_name = String::from("rgrIII_basic");
    let data = setup(&test_name, false, false);

    println!(
        "Running command: {} -t {} -w {} -d {}",
        &data.master, &data.test_file, &data.worker, &data.work_dir
    );

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&data.master])
        .with_args(&[
            "-t",
            &data.test_file,
            "-w",
            &data.worker,
            "-d",
            &data.work_dir,
        ])
        .succeeds()
        .unwrap();

    //Check the test ended with the correct number of processes.
    let master_log_file = format!(
        "{}{}{}{}{}",
        &data.work_dir,
        std::path::MAIN_SEPARATOR,
        LOG_DIR_NAME,
        std::path::MAIN_SEPARATOR,
        DEFAULT_MASTER_LOG
    );
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
    let master_node_num = logging::find_record_by_msg(
        "End_Test action: Finished. 25 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    //Check the handshake between the nodes
    let node25_log_file = format!("{}/log/node25.log", &data.work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();
    let received_packets = count_data_packets(&node25_log_records);
    assert_eq!(received_packets, 1);

    //Test passed. Results are not needed.
    teardown(data, true);
}

#[test]
fn test_route_discovery_optimization() {
    let test_name = String::from("rgrIII_route_discovery_optimization");
    let data = setup(&test_name, false, false);

    println!(
        "Running command: {} -t {} -w {} -d {}",
        &data.master, &data.test_file, &data.worker, &data.work_dir
    );

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&data.master])
        .with_args(&[
            "-t",
            &data.test_file,
            "-w",
            &data.worker,
            "-d",
            &data.work_dir,
        ])
        .succeeds()
        .unwrap();

    //Check the test ended with the correct number of processes.
    let master_log_file = format!(
        "{}{}{}{}{}",
        &data.work_dir,
        std::path::MAIN_SEPARATOR,
        LOG_DIR_NAME,
        std::path::MAIN_SEPARATOR,
        DEFAULT_MASTER_LOG
    );
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
    let master_node_num = logging::find_record_by_msg(
        "End_Test action: Finished. 81 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    //Node 43 should received 16 packets
    let node43_log_file = format!("{}/log/node43.log", &data.work_dir);
    let node43_log_records = logging::get_log_records_from_file(&node43_log_file).unwrap();
    let received_packets = count_data_packets(&node43_log_records);
    assert_eq!(received_packets, 16);

    //Node 38 should received 16 packets
    let node38_log_file = format!("{}/log/node38.log", &data.work_dir);
    let node38_log_records = logging::get_log_records_from_file(&node38_log_file).unwrap();
    let received_packets = count_data_packets(&node38_log_records);
    assert_eq!(received_packets, 16);

    //Node 45 should received 16 packets
    let node45_log_file = format!("{}/log/node45.log", &data.work_dir);
    let node45_log_records = logging::get_log_records_from_file(&node45_log_file).unwrap();
    let received_packets = count_data_packets(&node45_log_records);
    assert_eq!(received_packets, 16);

    //Test passed. Results are not needed.
    teardown(data, true);
}
//
//#[test]
//fn test_route_teardown() {
//    let test = get_test_path("rgr_route_teardown.toml");
//    let work_dir = create_test_dir("rgr_route_teardown");
//
//    let program = get_master_path();
//    let worker = get_worker_path();
//
//
//    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);
//
//    //Assert the test finished succesfully
//    assert_cli::Assert::command(&[&program])
//        .with_args(&["-t",  &test, "-w", &worker, "-d", &work_dir])
//        .succeeds()
//        .unwrap();
//
//    //Check the test ended with the correct number of processes.
//    let master_log_file = format!("{}{}{}{}{}", &work_dir,
//                                  std::path::MAIN_SEPARATOR,
//                                  LOG_DIR_NAME,
//                                  std::path::MAIN_SEPARATOR,
//                                  DEFAULT_MASTER_LOG);
//    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
//    let _master_node_num = logging::find_log_record("msg",
//                                                   "End_Test action: Finished. 23 processes terminated.",
//                                                   &master_log_records);
//
//    let node7_log_file = format!("{}/log/node7.log", &work_dir);
//    let node7_log_records = logging::get_log_records_from_file(&node7_log_file).unwrap();
//    let node7_teardown_recv = logging::find_log_record("msg", "Route TEARDOWN msg received for route 58dd5a265c14d7142b37a481ccd74608", &node7_log_records);
//
//
//    let mut received_packets = 0;
//    let node25_log_file = format!("{}/log/node25.log", &work_dir);
//    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();
//
//    for record in node25_log_records.iter() {
//        if record["msg"].as_str().unwrap().contains("reached its destination") {
//            received_packets += 1;
//        }
//    }
//
//    //3 packets should arrive before the route is torn-down
//    assert_eq!(received_packets, 3);
//    //Confirm the route disruption was detected and the source node received it.
//    assert!(node7_teardown_recv.is_some());
//
//    //Test passed. Results are not needed.
//    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
//}
//
