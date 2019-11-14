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
    let master_node_num = logging::find_record_by_msg(
                                                   "End_Test action: Finished. 25 processes terminated.", 
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    let node25_log_file = format!("{}/log/node25.log", &work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();

    let mut received_packets = 0 ;
    for record in node25_log_records.iter() {
        if record.status.is_some() && record.msg_type.is_some() {
            let status = &record.status.clone().unwrap();
            let msg_type = &record.msg_type.clone().unwrap();
            if status == "ACCEPTED" && msg_type == "DATA" {
                received_packets += 1;
            }
        } 
    }
    assert_eq!(received_packets, 2);

    //Test passed. Results are not needed.
    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}

#[test]
fn aodv_rerr() {
    let test = get_test_path("aodv_rerr.toml");
    let work_dir = create_test_dir("aodv_rerr");

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
    let master_node_num = logging::find_record_by_msg(
                                                   "End_Test action: Finished. 24 processes terminated.", 
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    let node5_log_file = format!("{}/log/node5.log", &work_dir);
    let node5_log_records = logging::get_log_records_from_file(&node5_log_file).unwrap();

    let mut received_packets = 0 ;
    let mut rerr_sent = false;
    for record in node5_log_records.iter() {
        //Check node5 received both data packets
        if record.status.is_some() && record.msg_type.is_some() {
            let status = &record.status.clone().unwrap();
            let msg_type = &record.msg_type.clone().unwrap();
            if status == "ACCEPTED" && msg_type == "DATA" {
                received_packets += 1;
            }
        } 
        //Check node5 detected node4 going down and sent an RERR
        if record.msg == "Sending message" && record.msg_type.is_some() {
            let msg_type = &record.msg_type.clone().unwrap();
            if  msg_type == "RERR" {
                rerr_sent = true;
            }
        } 
    }
    assert_eq!(received_packets, 2);
    assert!(rerr_sent);     

    let node3_log_file = format!("{}/log/node3.log", &work_dir);
    let node3_log_records = logging::get_log_records_from_file(&node3_log_file).unwrap();
    for record in node3_log_records.iter() {
        //Check node3 detected node4 going down and sent an RERR
        if record.msg == "Sending message" && record.msg_type.is_some() {
            let msg_type = &record.msg_type.clone().unwrap();
            if  msg_type == "RERR" {
                rerr_sent = true;
            }
        } 
    }
    assert!(rerr_sent);     

    let node9_log_file = format!("{}/log/node9.log", &work_dir);
    let node9_log_records = logging::get_log_records_from_file(&node9_log_file).unwrap();
    for record in node9_log_records.iter() {
        //Check node3 detected node4 going down and sent an RERR
        if record.msg == "Sending message" && record.msg_type.is_some() {
            let msg_type = &record.msg_type.clone().unwrap();
            if  msg_type == "RERR" {
                rerr_sent = true;
            }
        } 
    }
    assert!(rerr_sent);
    
    // //Test passed. Results are not needed.
    fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
}
