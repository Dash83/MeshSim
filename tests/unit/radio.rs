extern crate assert_cli;
extern crate chrono;
extern crate mesh_simulator;
extern crate socket2;

use std::env;

use super::super::*;
use mesh_simulator::logging;

use mesh_simulator::mobility2::*;
use mesh_simulator::tests::common::*;
use mesh_simulator::worker::radio::*;
use mesh_simulator::worker::worker_config::*;
use mesh_simulator::worker::*;

//**** Radio unit tests ****
//TODO: Implement test
// //Unit test for: Radio::new
// #[test]
// fn test_radio_new() {
//     let radio = Radio::new();
//     let radio_string = "Radio { delay: 0, reliability: 1, broadcast_groups: [], radio_name: \"\" }";

//     assert_eq!(format!("{:?}", radio), String::from(radio_string));
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
    use mesh_simulator::worker::protocols::naive_routing;

    //Setup
    //Get general test settings
    // let test_path = create_test_dir("sim_bcast");
    // let logger = logging::create_discard_logger();
    // println!("Test results placed in {}", &test_path);

    let data = setup("sim_bcast", false, true);
    // let _res = create_db_objects(&logger).expect("Could not create database objects");
    let conn = get_db_connection(&data.db_env_file.unwrap(), &data.logger)
        .expect("Could not get DB connection");

    //Worker1
    let mut sr_config1 = RadioConfig::new();
    sr_config1.range = 100.0;

    let work_dir = data.work_dir.clone();
    let worker_name = String::from("worker1");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
    let random_seed = 1;
    let (r1, l1) = sr_config1
        .create_radio(
            OperationMode::Simulated,
            RadioTypes::ShortRange,
            work_dir,
            worker_name.clone(),
            worker_id.clone(),
            random_seed,
            None,
            data.logger.clone(),
        )
        .expect("Could not create radio for worker1");

    // let listener1 = r1.init().unwrap();
    let pos = Position { x: -60.0, y: 0.0 };
    let vel = Velocity { x: 0.0, y: 0.0 };
    let _worker_db_id = register_worker(
        &conn,
        worker_name,
        worker_id,
        pos,
        vel,
        &None,
        Some(r1.get_address().into()),
        None,
        &data.logger,
    )
    .expect("Could not register worker");

    //Worker2
    let mut sr_config2 = RadioConfig::new();
    sr_config2.range = 100.0;
    let work_dir = data.work_dir.clone();
    let worker_name = String::from("worker2");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72b"); //arbitrary
    let random_seed = 1;
    let (r2, _l2) = sr_config2
        .create_radio(
            OperationMode::Simulated,
            RadioTypes::ShortRange,
            work_dir,
            worker_name.clone(),
            worker_id.clone(),
            random_seed,
            None,
            data.logger.clone(),
        )
        .expect("Could not create radio for worker2");
    // let _listener2 = r2.init().unwrap();

    let pos = Position { x: 0.0, y: 0.0 };
    let _worker_db_id = register_worker(
        &conn,
        worker_name,
        worker_id,
        pos,
        vel,
        &None,
        Some(r2.get_address().into()),
        None,
        &data.logger,
    )
    .expect("Could not register worker");

    //Worker3
    let mut sr_config3 = RadioConfig::new();
    sr_config3.range = 100.0;

    let work_dir = data.work_dir.clone();
    let worker_name = String::from("worker3");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72c"); //arbitrary
    let random_seed = 1;
    let (r3, l3) = sr_config3
        .create_radio(
            OperationMode::Simulated,
            RadioTypes::ShortRange,
            work_dir,
            worker_name.clone(),
            worker_id.clone(),
            random_seed,
            None,
            data.logger.clone(),
        )
        .expect("Could not create radio for worker3");

    // let listener3 = r3.init().unwrap();
    let pos = Position { x: 60.0, y: 0.0 };
    let _worker_db_id = register_worker(
        &conn,
        worker_name,
        worker_id,
        pos,
        vel,
        &None,
        Some(r3.get_address().into()),
        None,
        &data.logger,
    )
    .expect("Could not register worker");

    //Test checks
    let msg = naive_routing::Messages::Data(naive_routing::DataMessage::new(vec![]));
    let log_data = protocols::ProtocolMessages::Naive(msg.clone());
    let bcast_msg = MessageHeader::new(
        String::new(),
        String::new(),
        naive_routing::serialize_message(msg).expect("Could not serialize message"),
    );
    let tx = r2.broadcast(bcast_msg.clone()).unwrap();
    radio::log_tx(
        &&data.logger,
        tx,
        &bcast_msg.get_msg_id(),
        MessageStatus::SENT,
        &bcast_msg.sender,
        &bcast_msg.destination,
        log_data,
    );
    let rec_msg1 = l1.read_message();

    assert!(rec_msg1.is_some());

    let rec_msg3 = l3.read_message();

    assert!(rec_msg3.is_some());

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&data.work_dir).unwrap();
}

//I currently have no idea how to reliably test the timing of the broadcast across nodes
//Leaving this test here for debugging purposes.
#[ignore]
#[test]
fn test_broadcast_timing() {
    //Setup
    //Get general test settings
    let test = get_test_path("radio_broadcast_timing.toml");
    let data = setup("radio_broadcast_timing", false, false);
    println!("Test results placed in {}", &data.work_dir);

    let program = get_master_path();
    let worker = get_worker_path();

    println!(
        "Running command: {} -t {} -w {} -d {}",
        &program, &test, &worker, &data.work_dir
    );

    //Assert the test finished successfully
    assert_cli::Assert::command(&[&program])
        .with_args(&["-t", &test, "-w", &worker, "-d", &data.work_dir])
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
        "End_Test action: Finished. 5 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&data.work_dir).unwrap();
}

#[test]
fn test_mac_layer_basic() {
    let test_name = String::from("mac_layer_basic");
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

    let node4_log_file = &format!("{}/log/node4.log", &data.work_dir);
    let incoming_messages = logging::get_incoming_message_records(node4_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node5_log_file = &format!("{}/log/node5.log", &data.work_dir);
    let incoming_messages = logging::get_incoming_message_records(node5_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node19_log_file = &format!("{}/log/node19.log", &data.work_dir);
    let incoming_messages = logging::get_incoming_message_records(node19_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node20_log_file = &format!("{}/log/node20.log", &data.work_dir);
    let incoming_messages = logging::get_incoming_message_records(node20_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node22_log_file = &format!("{}/log/node22.log", &data.work_dir);
    let incoming_messages = logging::get_incoming_message_records(node22_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node24_log_file = &format!("{}/log/node24.log", &data.work_dir);
    let incoming_messages = logging::get_incoming_message_records(node24_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node25_log_file = &format!("{}/log/node25.log", &data.work_dir);
    let incoming_messages = logging::get_incoming_message_records(node25_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    teardown(data, true);
}

#[test]
fn test_broadcast_device() -> TestResult {
    use mesh_simulator::worker::protocols::naive_routing;

    //Setup
    let host = env::var("MESHSIM_HOST").unwrap_or(String::from(""));
    //This test should ONLY run on my lab development machine due to required configuration of device_mode.
    if !host.eq("kaer-morhen") {
        println!("This test should only run in the kaer-morhen host");
        return Ok(());
    }

    //Acquire the lock for the NIC since other tests also require it and they conflict with each other.
    let _nic = WIRELESS_NIC.lock()?;

    //Get general test settings
    let test_name = String::from("dev_bcast");
    let data = setup(&test_name, false, true);

    //init_logger(&test_path, "device_bcast_test");
    println!("Test results placed in {}", &data.work_dir);

    //Worker1
    let sr_config1 = RadioConfig::new();
    let work_dir = data.work_dir.clone();
    let worker_name = String::from("worker1");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
    let random_seed = 1;
    let (r1, l1) = sr_config1
        .create_radio(
            OperationMode::Device,
            RadioTypes::ShortRange,
            work_dir,
            worker_name,
            worker_id,
            random_seed,
            None,
            data.logger.clone(),
        )
        .expect("Could not create radio for worker1");
    // let listener1 = r1.init().unwrap();

    //Test checks
    let msg = naive_routing::Messages::Data(naive_routing::DataMessage::new(vec![]));
    let log_data = protocols::ProtocolMessages::Naive(msg.clone());
    let bcast_msg = MessageHeader::new(
        String::new(),
        String::new(),
        naive_routing::serialize_message(msg).expect("Could not serialize message"),
    );
    let tx = r1.broadcast(bcast_msg.clone()).unwrap();
    radio::log_tx(
        &&data.logger,
        tx,
        &bcast_msg.get_msg_id(),
        MessageStatus::SENT,
        &bcast_msg.sender,
        &bcast_msg.destination,
        log_data,
    );
    //We only test that the broadcast was received by the broadcaster, since we can only deploy 1 device_mode worker
    //per machine.
    let rec_msg1 = l1.read_message();
    assert!(rec_msg1.is_some());
    let rec_msg1 = rec_msg1.unwrap();

    assert_eq!(bcast_msg.get_payload(), rec_msg1.get_payload());

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    teardown(data, false);

    Ok(())
}

#[test]
fn test_last_transmission() -> TestResult {
    use mesh_simulator::worker::protocols::naive_routing;

    let test_name = "last_transmission";
    let data = setup(test_name, false, true);
    let conn =
        get_db_connection(&data.db_env_file.unwrap(), &data.logger).expect("Could not connect");

    let worker_name = String::from("node1");
    let worker_id = String::from("SOME_UNIQUE_ID");
    let random_seed = 12345;
    let config = RadioConfig::new();
    let (tx, _rx) = config
        .create_radio(
            OperationMode::Simulated,
            RadioTypes::ShortRange,
            data.work_dir.clone(),
            worker_name.clone(),
            worker_id.clone(),
            random_seed,
            // Some(Arc::clone(&rng)),
            None,
            data.logger.clone(),
        )
        .expect("Could not create radio-channels");
    let radio_address = tx.get_address();
    let pos = Position { x: 0.0, y: 0.0 };
    let vel = Velocity { x: 0.0, y: 0.0 };
    let dest = None;
    let _db_id = register_worker(
        &conn,
        worker_name,
        worker_id,
        pos,
        vel,
        &dest,
        Some(radio_address.to_string()),
        None,
        &data.logger,
    )
    .expect("Could not register worker");

    //Time before
    let ts1 = Utc::now();

    let msg = naive_routing::Messages::Data(naive_routing::DataMessage::new(vec![]));
    let log_data = protocols::ProtocolMessages::Naive(msg.clone());
    let hdr = MessageHeader::new(
        String::new(),
        String::new(),
        naive_routing::serialize_message(msg).expect("Could not serialize message"),
    );
    let tx_md = tx
        .broadcast(hdr.clone())
        .expect("Could not broadcast message");
    radio::log_tx(
        &data.logger,
        tx_md,
        &hdr.get_msg_id(),
        MessageStatus::SENT,
        &hdr.sender,
        &hdr.destination,
        log_data,
    );
    //Broadcast time
    let bc_ts = tx.last_transmission();

    //Now
    let ts2 = Utc::now();

    assert!(ts1.timestamp_nanos() < bc_ts);
    assert!(bc_ts < ts2.timestamp_nanos());

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&data.work_dir).unwrap();

    Ok(())
}
