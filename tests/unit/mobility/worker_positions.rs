extern crate mesh_simulator;

use std::time::Duration;

use diesel::PgConnection;
use mesh_simulator::tests::common::*;
use mesh_simulator::backend::*;

use super::common::*;

#[test]
/// Basic unit tests for update_worker_positions() in the absence of a
/// mobility model.
fn test_worker_positions_basic() {
    // Setup the test environment and create a database
    let mut data = setup("worker_positions_basic", false, true);

    // Get the postgresql connection
    let env_file = data.db_env_file.take()
        .expect("Failed to unwrap env_file");
    let conn = get_db_connection_by_file(env_file.clone(), &data.logger)
        .expect("Could not get DB connection");

    // Add a list of test workers to the mobility system
    let mut test_workers: Vec<MobilityTestWorker> = MobilityTestWorker::add_basic_mobility_test_workers(&data, &conn);

    // Test 1: update the worker positions until they reach their destination.
    let steps_to_destination = test_workers[0].get_steps_to_destination(None);
    for step in 1..steps_to_destination {
        println!("----- Iteration {}", step);

        // Update all worker positions
        let _res = update_worker_positions(&conn, 1.0)
            .expect("Failed to update worker positions.");

        // Verify the worker's position with respect to its previous position.
        verify_workers_state(&conn, &mut test_workers, None);
    }

    // Test 2: continue to update the worker positions after they reached their
    // destination. In the absence of a mobility model, the workers continue
    // to move past their destination with the same destination and velocity.
    for step in steps_to_destination..(steps_to_destination + 3) {
        println!("----- Iteration {}", step);

        // Update all worker positions
        let _res = update_worker_positions(&conn, 1.0)
            .expect("Failed to update worker positions.");

        // Verify the worker's position with respect to its previous position.
        verify_workers_state(&conn, &mut test_workers, None);
    }

    //Test passed. Results are not needed.
    teardown(data, true);
}

#[test]
/// Unit tests for select_workers_at_destination() using the default
/// mobility update period of 1 second 
fn test_workers_at_destination_basic() {
    let period = mesh_simulator::mobility::DEFAULT_MOBILITY_PERIOD;

    test_workers_at_destination("workers_at_destination_basic", period);
}

#[test]
/// Unit tests for select_workers_at_destination() using a
/// mobility update period of 3 seconds 
fn test_workers_at_destination_period_1() {
    let period = Duration::from_secs(3).as_nanos() as u64;

    test_workers_at_destination("workers_at_destination_period_1", period);
}

#[test]
/// Unit tests for select_workers_at_destination() using a
/// mobility update period of 0.4 seconds
fn test_workers_at_destination_period_2() {
    let period = Duration::from_millis(400).as_nanos() as u64;

    test_workers_at_destination("workers_at_destination_period_2", period);
}

/// Basic unit tests for select_workers_at_destination() in the absence of a
/// mobility model.
fn test_workers_at_destination(base_name: &str, period: u64) {
    let mut data = setup(base_name, false, true);
    // Worker update ratio
    let ratio = mobility_test_period_to_ratio(period);

    // Get the postgresql connection
    let env_file = data.db_env_file.take()
        .expect("Failed to unwrap env_file");
    let conn = get_db_connection_by_file(env_file.clone(), &data.logger)
        .expect("Could not get DB connection");

    // Add a list of test workers to the mobility system
    let mut test_workers = MobilityTestWorker::add_basic_mobility_test_workers(&data, &conn);

    // Update the worker positions until they reach their destination.
    let mut workers_at_destination = 0;
    while workers_at_destination < test_workers.len() {
        println!("----- Before Pause");

        // Update all worker positions
        let _res = update_worker_positions(&conn, ratio)
            .expect("Failed to update worker positions.");

        let newly_arrived = select_workers_at_destination(&conn, ratio)
            .expect("Failed to select workers at destination.");
        workers_at_destination += newly_arrived.len();
    
        // Verify the workers positions
        verify_workers_state(&conn, &mut test_workers, Some(period));
    }

    //Test passed. Results are not needed.
    teardown(data, true);
}

/// Sanity checks the node state of every worker with respect to its
/// previous position. A worker's behavior before reaching its original
/// destination is the same regardless of the mobility model.
fn verify_workers_state(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>, period: Option<u64>) {
    // Get the current position of every worker
    let worker_states = select_all_workers_state(&conn)
        .expect("Failed to select worker positions.");
    
    assert_eq!(worker_states.len(), test_workers.len(),
        "select_all_workers_state returned the wrong number of results.");

    for worker in test_workers.iter_mut() {
        // Find the worker's state by worker name
        let node_state = worker_states.iter().find(|&s|s.name.eq(&worker.name));
        assert!(node_state.is_some(),
            "Node state not found for worker {}", worker.name);
        let state = node_state.unwrap();
    
        println!("Worker {} \tat {},{} \tdest {},{} \tvel {},{}",
            state.name,
            state.pos.x, state.pos.y,
            state.dest.unwrap().x, state.dest.unwrap().y,
            state.vel.x, state.vel.y,);

        // Verify the worker's position
        worker.verify_state(&state, period);

        // Save the current position as the previous position
        worker.previous_position = state.pos;
    }
}
