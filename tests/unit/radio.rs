extern crate assert_cli;
extern crate chrono;
extern crate mesh_simulator;
extern crate socket2;



use std::env;

use self::mesh_simulator::logging;
use self::mesh_simulator::worker::worker_config::*;
use self::mesh_simulator::worker::*;
use self::mesh_simulator::worker::radio::*;
use super::super::*;
use mesh_simulator::logging::*;


//**** Radio unit tests ****
//TODO: Implement test
// //Unit test for: Radio::new
// #[test]
// fn test_radio_new() {
//     let radio = Radio::new();
//     let radio_string = "Radio { delay: 0, reliability: 1, broadcast_groups: [], radio_name: \"\" }";

//     assert_eq!(format!("{:?}", radio), String::from(radio_string));
// }

//TODO: Implement test
// //Unit test for: Radio::add_bcast_group
// #[test]
// fn test_radio_add_bcast_group() {
//     let mut radio = Radio::new();
//     radio.add_bcast_group(String::from("group1"));

//     assert_eq!(radio.broadcast_groups, vec![String::from("group1")]);
// }

// fn setup<'a>(path : &'a str ) {
//     use self::mesh_simulator::worker::mobility::*;

//     let db_path = format!("{}{}{}", path, std::path::MAIN_SEPARATOR, &DB_NAME);
//     println!("DB path: {}", &db_path);
//     let conn = get_db_connection(&db_path).expect("Could not create DB file");
//     let _res = create_db_objects(&conn).expect("Could not create positions table");
// }

//TODO: Review if this test is still needed.
// //Unit test for: Radio::scan_for_peers
// #[test]
// fn test_scan_for_peers_simulated() {
//     use self::mesh_simulator::worker::mobility::*;

//     //Setup
//     //Get general test settings
//     let test_path = create_test_dir("sim_scan");
//     println!("Test results placed in {}", &test_path);

//     let conn = get_db_connection(&test_path).expect("Could not create DB file");
//     let _res = create_positions_db(&conn).expect("Could not create positions table");

//     //Worker1
//     let mut sr_config1 = RadioConfig::new();
//     sr_config1.range = 100.0;
//     let work_dir = test_path.clone();
//     let worker_name = String::from("worker1");
//     let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
//     let random_seed = 1;
//     let r1 = sr_config1.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name.clone(), 
//                                      worker_id.clone(), random_seed, None);
//     let _listener1 = r1.init().unwrap();
//     let pos = Position{ x : -60.0, y : 0.0};
//     let _worker_db_id = register_worker(&conn, worker_name, 
//                                                &worker_id, 
//                                                &pos, 
//                                                Some(r1.get_address().into()), 
//                                                None).expect("Could not register Worker1");

//     //Worker2
//     let mut sr_config2 = RadioConfig::new();
//     sr_config2.range = 100.0;
//     sr_config2.broadcast_groups.clear();
//     sr_config2.broadcast_groups.push(String::from("group1"));
//     sr_config2.broadcast_groups.push(String::from("group2"));
//     let work_dir = test_path.clone();
//     let worker_name = String::from("worker2");
//     let worker_id = String::from("416d77337e24399dc7a5aa058039f72b"); //arbitrary
//     let random_seed = 1;
//     let r2 = sr_config2.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name.clone(), 
//                                      worker_id.clone(), random_seed, None);
//     let _listener2 = r2.init().unwrap();
//     let pos = Position{ x : 0.0, y : 0.0};
//     let _worker_db_id = register_worker(&conn, worker_name, 
//                                                &worker_id, 
//                                                &pos, 
//                                                Some(r2.get_address().into()), 
//                                                None).expect("Could not register Worker2");
    
//     //Worker3
//     let mut sr_config3= RadioConfig::new();
//     sr_config3.range = 100.0;
//     sr_config3.broadcast_groups.clear();
//     sr_config3.broadcast_groups.push(String::from("group2"));
//     sr_config3.broadcast_groups.push(String::from("group3"));
//     let work_dir = test_path.clone();
//     let worker_name = String::from("worker3");
//     let worker_id = String::from("416d77337e24399dc7a5aa058039f72c"); //arbitrary
//     let random_seed = 1;
//     let r3 = sr_config3.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name.clone(), 
//                                      worker_id.clone(), random_seed, None);
//     let _listener3 = r3.init().unwrap();
//     let pos = Position{ x : 60.0, y : 0.0};
//     let _worker_db_id = register_worker(&conn, worker_name, 
//                                                &worker_id, 
//                                                &pos, 
//                                                Some(r3.get_address().into()), 
//                                                None).expect("Could not register Worker3");
//     //Test checks
//     let peers1 = r1.scan_for_peers().unwrap(); 
//     assert_eq!(peers1.len(), 1); //Should detect worker2

//     let peers2 = r2.scan_for_peers().unwrap(); 
//     assert_eq!(peers2.len(), 2); //Should detect worker1 and worker 3

//     let peers3 = r3.scan_for_peers().unwrap(); 
//     assert_eq!(peers3.len(), 1); //Should detect worker2


//     //Teardown
//     //If test checks fail, this section won't be reached and not cleaned up for investigation.
//     let _res = std::fs::remove_dir_all(&test_path).unwrap();
// }

//TODO: Implement
// //Unit test for: Radio::scan_for_peers
// #[test]
// fn test_scan_for_peers_device() { 

// }

#[test]
fn test_broadcast_simulated() {
    use self::mesh_simulator::worker::mobility::*;

    //Setup
    //Get general test settings
    let test_path = create_test_dir("sim_bcast");
    let logger = logging::create_discard_logger();
    println!("Test results placed in {}", &test_path);

    let conn = get_db_connection(&test_path, &logger).expect("Could not create DB file");
    let _res = create_db_objects(&conn, &logger).expect("Could not create positions table");

    //Worker1
    let mut sr_config1 = RadioConfig::new();
    sr_config1.range = 100.0;

    let work_dir = test_path.clone();
    let worker_name = String::from("worker1");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
    let random_seed = 1;
    let (r1, l1) = sr_config1.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name.clone(),
                                     worker_id.clone(), random_seed, None, logger.clone()).expect("Could not create radio for worker1");

    // let listener1 = r1.init().unwrap();
    let pos = Position{ x : -60.0, y : 0.0};
    let vel = Velocity{ x : 0.0, y : 0.0};
    let _worker_db_id = register_worker(&conn, worker_name,
                                               &worker_id,
                                               &pos,
                                               &vel,
                                               &None,
                                               Some(r1.get_address().into()),
                                               None,
                                               &logger).expect("Could not register worker");

    //Worker2
    let mut sr_config2 = RadioConfig::new();
    sr_config2.range = 100.0;
    let work_dir = test_path.clone();
    let worker_name = String::from("worker2");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72b"); //arbitrary
    let random_seed = 1;
    let (r2, _l2) = sr_config2.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name.clone(),
                                     worker_id.clone(), random_seed, None, logger.clone()).expect("Could not create radio for worker2");
    // let _listener2 = r2.init().unwrap();

    let pos = Position{ x : 0.0, y : 0.0};
    let _worker_db_id = register_worker(&conn, worker_name,
                                               &worker_id,
                                               &pos,
                                               &vel,
                                               &None,
                                               Some(r2.get_address().into()),
                                               None,
                                               &logger).expect("Could not register worker");

    //Worker3
    let mut sr_config3= RadioConfig::new();
    sr_config3.range = 100.0;

    let work_dir = test_path.clone();
    let worker_name = String::from("worker3");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72c"); //arbitrary
    let random_seed = 1;
    let (r3, l3) = sr_config3.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name.clone(),
                                     worker_id.clone(), random_seed, None, logger.clone()).expect("Could not create radio for worker3");

    // let listener3 = r3.init().unwrap();
    let pos = Position{ x : 60.0, y : 0.0};
    let _worker_db_id = register_worker(&conn, worker_name,
                                               &worker_id,
                                               &pos,
                                               &vel,
                                               &None,
                                               Some(r3.get_address().into()),
                                               None,
                                               &logger).expect("Could not register worker");

    //Test checks
    let bcast_msg = MessageHeader::new();
    let _res = r2.broadcast(bcast_msg).unwrap();

    let rec_msg1 = l1.read_message();

    assert!(rec_msg1.is_some());

    let rec_msg3 = l3.read_message();

    assert!(rec_msg3.is_some());

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&test_path).unwrap();
}

//I currently have no idea how to reliably test the timing of the broadcast across nodes
//Leaving this test here for debugging purposes.
#[ignore]
#[test]
fn test_broadcast_timing() {
    //Setup
    //Get general test settings
    let test = get_test_path("radio_broadcast_timing.toml");
    let work_dir = create_test_dir("radio_broadcast_timing");

    println!("Test results placed in {}", &work_dir);

    let program = get_master_path();
    let worker = get_worker_path();

    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    //Assert the test finished successfully
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
                                                   "End_Test action: Finished. 5 processes terminated.",
                                                   &master_log_records);
    assert!(master_node_num.is_some());

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&work_dir).unwrap();
}

#[test]
fn test_mac_layer_basic() {
    //Setup
    //Get general test settings
    let test = get_test_path("mac_layer_basic.toml");
    let work_dir = create_test_dir("mac_layer_basic");

    println!("Test results placed in {}", &work_dir);

    let program = get_master_path();
    let worker = get_worker_path();

    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    //Assert the test finished successfully
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

    let node4_log_file = &format!("{}/log/node4.log", &work_dir);
    let node4_log_records = logging::get_log_records_from_file(&node4_log_file).unwrap();
    let node_4_received = logging::find_record_by_msg(
        "Received message db805e19ab4bfaa5cef9b146993859a9", 
        &node4_log_records
    );
    assert!(node_4_received.is_some() && 
            node_4_received.cloned().unwrap().status.unwrap() == "ACCEPTED");

    let node5_log_file = &format!("{}/log/node5.log", &work_dir);
    let node5_log_records = logging::get_log_records_from_file(&node5_log_file).unwrap();
    let node_5_received = logging::find_record_by_msg(
        "Received message 920d4c99eb3e393e47fa84268a559abb", 
        &node5_log_records
    );
    assert!(node_5_received.is_some() && 
            node_5_received.cloned().unwrap().status.unwrap() == "ACCEPTED");

    let node19_log_file = &format!("{}/log/node19.log", &work_dir);
    let node19_log_records = logging::get_log_records_from_file(&node19_log_file).unwrap();
    let node_19_received = logging::find_record_by_msg(
        "Received message f57790d905cdfd41887f18c6162b99bf", 
        &node19_log_records
    );
    assert!(node_19_received.is_some() && 
            node_19_received.cloned().unwrap().status.unwrap() == "ACCEPTED");

    let node20_log_file = &format!("{}/log/node20.log", &work_dir);
    let node20_log_records = logging::get_log_records_from_file(&node20_log_file).unwrap();
    let node_20_received = logging::find_record_by_msg(
        "Received message d763e55c7fd41dd1d635ca7e6602e855", 
        &node20_log_records
    );
    assert!(node_20_received.is_some() && 
            node_20_received.cloned().unwrap().status.unwrap() == "ACCEPTED");
    
    let node22_log_file = &format!("{}/log/node22.log", &work_dir);
    let node22_log_records = logging::get_log_records_from_file(&node22_log_file).unwrap();
    let node_22_received = logging::find_record_by_msg(
        "Received message d9802591b0658377c3ce50e56822bfe2", 
        &node22_log_records
    );
    assert!(node_22_received.is_some() && 
            node_22_received.cloned().unwrap().status.unwrap() == "ACCEPTED");

    let node24_log_file = &format!("{}/log/node24.log", &work_dir);
    let node24_log_records = logging::get_log_records_from_file(&node24_log_file).unwrap();
    let node_24_received = logging::find_record_by_msg(
        "Received message eadd0fef2df22db64ac06de822ae5c15", 
        &node24_log_records
    );
    assert!(node_24_received.is_some() && 
            node_24_received.cloned().unwrap().status.unwrap() == "ACCEPTED");
    
    let node25_log_file = &format!("{}/log/node25.log", &work_dir);
    let node25_log_records = logging::get_log_records_from_file(&node25_log_file).unwrap();
    let node_25_received = logging::find_record_by_msg(
        "Received message fbedcdf252b06a96e4b6b7323be7d788", 
        &node25_log_records
    );
    assert!(node_25_received.is_some() && 
            node_25_received.cloned().unwrap().status.unwrap() == "ACCEPTED");
    
    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&work_dir).unwrap();
}

#[test]
fn test_broadcast_device() -> TestResult {
    //Setup
    let host = env::var("MESHSIM_HOST").unwrap_or(String::from(""));
    //This test should ONLY run on my lab development machine due to required configuration of device_mode.
    if !host.eq("kaer-morhen") {
        println!("This test should only run in the kaer-morhen host");
        return Ok(())
    }

    //Get general test settings
    let test_path = create_test_dir("dev_bcast");
    let log_file = format!("{}{}test.log", &test_path, std::path::MAIN_SEPARATOR);
    let logger = logging::create_logger(&log_file, false).expect("Could not create logger");
    
    //Acquire the lock for the NIC since other tests also require it and they conflict with each other. 
    let _nic = WIRELESS_NIC.lock()?;
    
    //init_logger(&test_path, "device_bcast_test");
    println!("Test results placed in {}", &test_path);
    
    //Worker1
    let sr_config1 = RadioConfig::new();
    let work_dir = test_path.clone();
    let worker_name = String::from("worker1");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
    let random_seed = 1;
    let (r1, l1) = sr_config1.create_radio(OperationMode::Device, RadioTypes::ShortRange, work_dir, worker_name, 
                                     worker_id, random_seed, None, logger.clone()).expect("Could not create radio for worker1");
    // let listener1 = r1.init().unwrap();

    //Test checks
    let bcast_msg = MessageHeader::new();
    let _res = r1.broadcast(bcast_msg.clone()).unwrap();

    //We only test that the broadcast was received by the broadcaster, since we can only deploy 1 device_mode worker
    //per machine.
    let rec_msg1 = l1.read_message();
    assert!(rec_msg1.is_some());
    let rec_msg1 = rec_msg1.unwrap();

    assert_eq!(bcast_msg.payload, rec_msg1.payload);

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&test_path).unwrap();

    Ok(())
}

