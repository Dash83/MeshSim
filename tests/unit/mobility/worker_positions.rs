extern crate mesh_simulator;

use std::time::Duration;

use diesel::PgConnection;
use mesh_simulator::mobility::NodeState;
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
    let mut test_workers: Vec<MobilityTestWorker> = MobilityTestWorker::generate_test_workers_basic();
    for test_worker in test_workers.iter(){
        test_worker.add_to_mobility_system(&data, &conn);
    }

    // Test 1: update the worker positions until they reach their destination.
    let steps_to_destination = test_workers[0].get_steps_to_destination(None);
    for step in 1..steps_to_destination {
        println!("----- Iteration {}", step);

        // Update all worker positions
        let _res = update_worker_positions(&conn, 1.0)
            .expect("Failed to update worker positions.");

        // Verify the worker's position with respect to its previous position.
        verify_workers_state(&conn, &mut test_workers, None, None);
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
        verify_workers_state(&conn, &mut test_workers, None, None);
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
    let mut test_workers = MobilityTestWorker::generate_test_workers_basic();
    for test_worker in test_workers.iter(){
        test_worker.add_to_mobility_system(&data, &conn);
    }

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
    
        verify_workers_state(&conn, &mut test_workers, Some(&newly_arrived), Some(period));
    }

    //Test passed. Results are not needed.
    teardown(data, true);
}

/// Sanity checks the current position of every worker with respect to its
/// previous position. The worker's behavior before reaching its original
/// destination is the same regardless of the mobility model.
pub fn verify_workers_state(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>, newly_arrived: Option<&Vec<NodeState>>, period: Option<u64>) {
    // Get the current position of every worker
    let worker_states = select_all_workers_state(&conn)
        .expect("Failed to select worker positions.");
    
    // The result must contain every worker in the test
    assert!(worker_states.len() == test_workers.len());

    for test_worker in test_workers.iter_mut() {
        // Find the worker's state by worker name
        let state = worker_states.iter().find(|&s|s.name.eq(&test_worker.name));
        assert!(state.is_some());
        let worker_state = state.unwrap();
    
        println!("Worker {} \tat {},{} \tdest {},{} \tvel {},{}",
            worker_state.name,
            worker_state.pos.x, worker_state.pos.y,
            worker_state.dest.unwrap().x, worker_state.dest.unwrap().y,
            worker_state.vel.x, worker_state.vel.y,);
    
        let is_at_destination = test_worker.is_next_at_destination(period);
    
        if newly_arrived.is_some() {
            // Verify whether this worker was correctly reported as newly
            // arrived at its destination.
            let newly_arrived_state = newly_arrived.unwrap().iter().find(|&s|s.name.eq(&test_worker.name));

            if is_at_destination && worker_state.pos.ne(&test_worker.previous_position) {
                assert!(newly_arrived_state.is_some());
            } 
            else {
                assert!(newly_arrived_state.is_none());
            }
        }
        
        // Verify the worker's destination and velocity.
        // Before reaching its destination, the worker must keep its original
        // velocity and destination regardless of the mobility model.
        if !is_at_destination {
            assert!(worker_state.dest.unwrap() == test_worker.destination);
            assert!(worker_state.vel == test_worker.velocity);
        }

        // Verify the worker's new position.
        let expected_position = test_worker.get_next_position(period);
        assert!(worker_state.pos == expected_position);

        // Save the current position as the previous position
        test_worker.previous_position = worker_state.pos;

        if is_at_destination {
            // Update the destination and velocity, the mobility model sets
            // these new values once the pause time has elapsed.
            test_worker.destination = worker_state.dest.unwrap();
            test_worker.velocity = worker_state.vel;
        }
    }
}
