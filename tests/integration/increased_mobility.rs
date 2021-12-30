extern crate mesh_simulator;

use super::super::*;

use mesh_simulator::tests::common::*;
use mesh_simulator::master::test_specification::TestSpec;

/// Basic test for increased mobility.
/// Verifies the initial and final position of a node with constant velocity.
#[test]
fn basic_test() {
    let test_name = String::from("increased_mobility_basic");
    // Create the test data from the increased_mobility_basic.toml file.
    let data = setup(&test_name, false, false);
    let test_spec = TestSpec::parse_test_spec(&data.test_file).expect("Failed to parse test file.");

    println!(
        "Running command: {} -t {} -w {} -d {}",
        &data.master, &data.test_file, &data.worker, &data.work_dir
    );

    // Run the test and assert it finished succesfully.
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

    let master_log_file = format!(
        "{}{}{}{}{}",
        &data.work_dir,
        std::path::MAIN_SEPARATOR,
        LOG_DIR_NAME,
        std::path::MAIN_SEPARATOR,
        DEFAULT_MASTER_LOG
    );
    
    verify_process_count(&test_spec, &master_log_file);

    // Node1 travels horizontally left to right.
    verify_node_mobility(&test_spec, &master_log_file, &String::from("node1"));

    // Node2 travels upwards vertically.
    verify_node_mobility(&test_spec, &master_log_file, &String::from("node2"));

    // Node3 travels horizontally right to left.
    verify_node_mobility(&test_spec, &master_log_file, &String::from("node3"));

    // Node4 travels downwards vertically.
    verify_node_mobility(&test_spec, &master_log_file, &String::from("node4"));

    //Test passed. Results are not needed.
    teardown(data, true);
}

#[test]
fn one_node() {
    let test_name = String::from("increased_mobility");
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
        "End_Test action: Finished. 1 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    let test_spec = TestSpec::parse_test_spec(&data.test_file).expect("Could not parse test file");
    let mobility_records = logging::get_mobility_records(&master_log_file).expect("Could not load mobility records");

    //Initial position
    let (pos0, vel0) = mobility_records["node1"]
        .iter()
        .find(|_x| true)
        .map(|(_ts, (pos, vel))| (*pos, *vel))
        .unwrap();

    let dist = mobility_records["node1"].iter()
    .scan(pos0, |prev, (_ts, (pos, _vel))| { 
        let dist = prev.distance(pos);
        *prev = *pos;
        Some(dist)
    })
    .fold(0., |acc, d| acc + d ); 

    let no_accel_dist = (test_spec.duration as f64 / 1000.0 )* vel0.magnitude();
    println!("Distance traveled by node: {}", dist);
    println!("Distance without increase: {}", no_accel_dist);

    assert!(dist > no_accel_dist);

    //Test passed. Results are not needed.
    teardown(data, true);
}

/// Asserts that the test finished with the correct number of processes.
fn verify_process_count(test_spec: &TestSpec, master_log_file: &String) {
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();

    // The expected number of processes is derived from the number of initial nodes in the test spec.
    let node_count = test_spec.initial_nodes.len();
    let end_msg = format!("End_Test action: Finished. {} processes terminated.", node_count);

    let master_node_num = logging::find_record_by_msg(
        &end_msg,
        &master_log_records,
    );
    assert!(master_node_num.is_some());
}

/// Verify the mobility records of the specified node based on the test specs.
fn verify_node_mobility(test_spec: &TestSpec, master_log_file: &String, node_name: &String) {
    // Read the mobility records.
    let mobility_records = logging::get_mobility_records(master_log_file).expect("Could not load mobility records");

    let test_area = test_spec.area_size;

    println!("Checking {}", node_name);
    assert!(mobility_records.contains_key(node_name));

    // Get the mobility records for the requested node.
    let node_records = &mobility_records[node_name];

    // Verify the node's initial position in the mobility records is consistent with the test specs.
    let (_ts_a, (pos_a, _vel_a)) = node_records.iter().next().unwrap();
    let initial_position = test_spec.initial_nodes[node_name].position;
    println!("Initial position: {},{}", pos_a.x, pos_a.y);
    assert_eq!(pos_a, &initial_position);

    // Calculate the distance traveled by the node as the sum of the partial position changes in the mobility records.
    let distance = node_records.iter()
        .scan(pos_a, |prev, (_ts, (pos, _vel))| { 
            let dist = prev.distance(pos);
            println!("Partial distance: {}, from {},{} to {},{}", dist, prev.x, prev.y, pos.x, pos.y);

            // Assert the node is positioned within the test area.
            // Note that a failure here would indicate a bad .toml configuration rather than an error in the code.
            // The velocity vector should move the node in the direction of its destination within the area.
            assert!(pos.x <= test_area.width);
            assert!(pos.y <= test_area.height);

            *prev = pos;
            Some(dist)
        })
        .fold(0., |acc, d| acc + d ); 

    // Verify the node reached its destination.
    // This test expects the test specs to specify a suitable velocity for the node to reach its destination.
    let (_ts_b, (pos_b, _vel_b)) = node_records.iter().last().unwrap();
    let destination = test_spec.initial_nodes[node_name].destination.unwrap();
    println!("Final position: {},{}", pos_b.x, pos_b.y);
    assert_eq!(pos_b, &destination);

    // Verify that the traveled distance is consistent with the configured destination.
    let expected_distance = destination.distance(&initial_position);
    println!("Distance traveled: {}, expected: {}", distance, expected_distance);
    assert_eq!(distance, expected_distance);
}