extern crate assert_cli;
extern crate chrono;
extern crate mesh_simulator;
extern crate socket2;

use self::chrono::prelude::*;
use std::path::{PathBuf, Path};
use std::env;
use std::fs;
use self::mesh_simulator::logging;
use self::mesh_simulator::worker::worker_config::*;
use self::mesh_simulator::worker::*;
use self::mesh_simulator::worker::radio::*;
use super::super::*;
use std::net:: SocketAddr;
use self::socket2::{Socket, SockAddr, Domain, Type, Protocol};

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

//Unit test for: Radio::scan_for_peers
#[test]
fn test_scan_for_peers_simulated() {
    //Setup
    //Get general test settings
    let test_path = create_test_dir("sim_scan");
    println!("Test results placed in {}", &test_path);

    //Worker1
    let mut sr_config1 = RadioConfig::new();
    sr_config1.broadcast_groups.clear();
    sr_config1.broadcast_groups.push(String::from("group1"));
    let work_dir = test_path.clone();
    let worker_name = String::from("worker1");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
    let random_seed = 1;
    let r1 = sr_config1.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name, 
                                     worker_id, random_seed, None);
    let _listener1 = r1.init().unwrap();

    //Worker2
    let mut sr_config2 = RadioConfig::new();
    sr_config2.broadcast_groups.clear();
    sr_config2.broadcast_groups.push(String::from("group1"));
    sr_config2.broadcast_groups.push(String::from("group2"));
    let work_dir = test_path.clone();
    let worker_name = String::from("worker2");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72b"); //arbitrary
    let random_seed = 1;
    let r2 = sr_config2.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name, 
                                     worker_id, random_seed, None);
    let _listener2 = r2.init().unwrap();
    
    //Worker3
    let mut sr_config3= RadioConfig::new();
    sr_config3.broadcast_groups.clear();
    sr_config3.broadcast_groups.push(String::from("group2"));
    sr_config3.broadcast_groups.push(String::from("group3"));
    let work_dir = test_path.clone();
    let worker_name = String::from("worker3");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72c"); //arbitrary
    let random_seed = 1;
    let r3 = sr_config3.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name, 
                                     worker_id, random_seed, None);
    let _listener3 = r3.init().unwrap();

    //Test checks
    let peers1 = r1.scan_for_peers().unwrap(); 
    assert_eq!(peers1.len(), 1); //Should detect worker2

    let peers2 = r2.scan_for_peers().unwrap(); 
    assert_eq!(peers2.len(), 2); //Should detect worker1 and worker 3

    let peers3 = r3.scan_for_peers().unwrap(); 
    assert_eq!(peers3.len(), 1); //Should detect worker2


    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&test_path).unwrap();
}

//TODO: Implement
// //Unit test for: Radio::scan_for_peers
// #[test]
// fn test_scan_for_peers_device() { 

// }

#[test]
fn test_broadcast_simulated() {
    //Setup
    //Get general test settings
    let test_path = create_test_dir("sim_bcast");
    println!("Test results placed in {}", &test_path);

    //Worker1
    let mut sr_config1 = RadioConfig::new();
    sr_config1.broadcast_groups.clear();
    sr_config1.broadcast_groups.push(String::from("group1"));
    let work_dir = test_path.clone();
    let worker_name = String::from("worker1");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
    let random_seed = 1;
    let r1 = sr_config1.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name, 
                                     worker_id, random_seed, None);
    let listener1 = r1.init().unwrap();

    //Worker2
    let mut sr_config2 = RadioConfig::new();
    sr_config2.broadcast_groups.clear();
    sr_config2.broadcast_groups.push(String::from("group1"));
    sr_config2.broadcast_groups.push(String::from("group2"));
    let work_dir = test_path.clone();
    let worker_name = String::from("worker2");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72b"); //arbitrary
    let random_seed = 1;
    let r2 = sr_config2.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name, 
                                     worker_id, random_seed, None);
    let _listener2 = r2.init().unwrap();
    
    //Worker3
    let mut sr_config3= RadioConfig::new();
    sr_config3.broadcast_groups.clear();
    sr_config3.broadcast_groups.push(String::from("group2"));
    sr_config3.broadcast_groups.push(String::from("group3"));
    let work_dir = test_path.clone();
    let worker_name = String::from("worker3");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72c"); //arbitrary
    let random_seed = 1;
    let r3 = sr_config3.create_radio(OperationMode::Simulated, RadioTypes::ShortRange, work_dir, worker_name, 
                                     worker_id, random_seed, None);
    let listener3 = r3.init().unwrap();

    //Test checks
    let bcast_msg = MessageHeader::new();
    let _res = r2.broadcast(bcast_msg).unwrap();

    let rec_msg1 = listener1.read_message();
    assert!(rec_msg1.is_some());

    let rec_msg3 = listener3.read_message();
    assert!(rec_msg3.is_some());

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&test_path).unwrap();
}

#[test]
fn test_broadcast_device() -> TestResult {
    //Setup
    //Get general test settings
    let test_path = create_test_dir("dev_bcast");
    let host = env::var("MESHSIM_HOST").unwrap_or(String::from(""));
    //This test should ONLY run on my lab development machine due to required configuration of device_mode.
    if !host.eq("kaer-morhen") {
        panic!("This test should only run in the kaer-morhen host");
    }
    
    //Acquire the lock for the NIC since other tests also require it and they conflict with each other. 
    let _nic = WIRELESS_NIC.lock()?;
    
    //init_logger(&test_path, "device_bcast_test");
    println!("Test results placed in {}", &test_path);
    
    //Worker1
    let mut sr_config1 = RadioConfig::new();
    sr_config1.broadcast_groups.clear();
    sr_config1.interface_name = Some(String::from("eno1"));
    let work_dir = test_path.clone();
    let worker_name = String::from("worker1");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
    let random_seed = 1;
    let r1 = sr_config1.create_radio(OperationMode::Device, RadioTypes::ShortRange, work_dir, worker_name, 
                                     worker_id, random_seed, None);
    let listener1 = r1.init().unwrap();

    //Test checks
    let bcast_msg = MessageHeader::new();
    let _res = r1.broadcast(bcast_msg.clone()).unwrap();
    println!("Multicast message sent");

    //We only test that the broadcast was received by the broadcaster, since we can only deploy 1 device_mode worker
    //per machine.
    let rec_msg1 = listener1.read_message();
    assert!(rec_msg1.is_some());
    let rec_msg1 = rec_msg1.unwrap();

    assert_eq!(bcast_msg.payload, rec_msg1.payload);

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&test_path).unwrap();

    Ok(())
}

