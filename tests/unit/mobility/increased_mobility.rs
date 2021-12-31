extern crate mesh_simulator;

use core::time;
use std::time::Duration;

use diesel::PgConnection;
use mesh_simulator::mobility::*;
use mesh_simulator::tests::common::*;
use mesh_simulator::backend::*;

use crate::unit::mobility::{common::{MobilityTestWorker, MOBILITY_TEST_AREA}, worker_positions::verify_workers_state};

use super::common::mobility_test_period_to_ratio;

#[test]
/// Basic unit tests for the handle_iteration() method of the IncreasedMobility
/// mobility handler.
fn test_increased_mobility_basic() {
    // Run the test
    test_increased_mobility("increased_mobility_basic", mesh_simulator::mobility::DEFAULT_MOBILITY_PERIOD);
}

#[test]
/// Unit tests for the handle_iteration() method of the IncreasedMobility
/// mobility handler with a 2 second mobility period (see MOBILITY_PERIOD).
fn test_increased_mobility_period_1() {
    let period = Duration::from_secs(2).as_nanos() as u64;

    // Run the test with a 2 second update period
    test_increased_mobility("increased_mobility_period_1", period);
}

#[test]
/// Unit tests for the handle_iteration() method of the IncreasedMobility
/// mobility handler with a 0.5 second mobility period (see MOBILITY_PERIOD).
fn test_increased_mobility_period_2() {
    let period = Duration::from_millis(500).as_nanos() as u64;

    // Run the test with a .5 second update period
    test_increased_mobility("increased_mobility_period_2", period);
}

/// Runs the basic tests for increased mobility
fn test_increased_mobility(base_name: &str, period: u64) {
    // Time in milliseconds that the workers will pause upon reaching their destination
    let pause_time: i64 = 10000;
    // Setup the test environment and create a database
    let mut data = setup(base_name, false, true);
    // Worker update ratio
    let ratio = mobility_test_period_to_ratio(period);

    // Get the postgresql connection
    let env_file = data.db_env_file.take()
        .expect("Failed to unwrap env_file");
    let conn = get_db_connection_by_file(env_file.clone(), &data.logger)
        .expect("Could not get DB connection");
    let conn2 = get_db_connection_by_file(env_file, &data.logger)
        .expect("Could not get DB connection");

    // Create the mobility handler
    let increased_mobility_handler = IncreasedMobility::new(
        0.0,
        43,
        MOBILITY_TEST_AREA,
        pause_time,
        conn2,
        data.logger.clone(),
        Some(period),
    );
    let mut mobility_handler: Box<dyn MobilityHandler> = Box::new(increased_mobility_handler);
    assert_eq!(mobility_handler.get_mobility_period(), period);

    // Add a list of test workers to the mobility system
    let mut test_workers = MobilityTestWorker::add_basic_mobility_test_workers(&data, &conn);

    // First, update the worker positions until they reach their destination.
    let mut workers_at_destination = 0;
    while workers_at_destination < test_workers.len() {
        println!("----- Before Pause");

        // Update all worker positions
        let _res = update_worker_positions(&conn, ratio)
            .expect("Failed to update worker positions.");
    
        // Pass control to mobility model to recalculate the worker positions
        let newly_arrived = mobility_handler.handle_iteration()
            .expect("Mobility iteration failed.");
        workers_at_destination += newly_arrived.len();
    
        verify_workers_state(&conn, &mut test_workers, Some(&newly_arrived), Some(period));
    }

    // The workers must be paused after reaching their destination, verify they
    // don't move during these updates
    for _step in 0..3 {
        println!("----- On Pause");

        // Update all worker positions
        let _res = update_worker_positions(&conn, ratio)
            .expect("Failed to update worker positions.");
    
        // Pass control to mobility model to recalculate the worker positions
        let newly_arrived = mobility_handler.handle_iteration()
            .expect("Mobility iteration failed.");

        verify_state_on_pause(&conn, &mut test_workers, &newly_arrived);
    }

    std::thread::sleep(time::Duration::from_millis(pause_time as u64));

    // After the pause, the workers remain in the same position and the
    // mobility system sets a new destination and velocity.
    {
        println!("----- On Pause");

        // Update all worker positions
        let _res = update_worker_positions(&conn, ratio)
            .expect("Failed to update worker positions.");
    
        // Pass control to mobility model to recalculate the worker positions
        let newly_arrived = mobility_handler.handle_iteration()
            .expect("Mobility iteration failed.");

        verify_state_on_pause(&conn, &mut test_workers, &newly_arrived);
    }

    // Verify that workers restart movement after the pause.
    for _step in 0..3 {
        println!("----- After pause");

        // Update all worker positions
        let _res = update_worker_positions(&conn, ratio)
            .expect("Failed to update worker positions.");
    
        // Pass control to mobility model to recalculate the worker positions
        let newly_arrived = mobility_handler.handle_iteration()
            .expect("Mobility iteration failed.");
    
        verify_workers_state(&conn, &mut test_workers, Some(&newly_arrived), Some(period));
    }

    //Test passed. Results are not needed.
    teardown(data, true);
}

/// Verifies that workers stay at their previous position
fn verify_state_on_pause(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>, newly_arrived: &Vec<NodeState>) {
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
        let newly_arrived_state = newly_arrived.iter().find(|&s|s.name.eq(&test_worker.name));

        println!("Worker {} \tat {},{} \tdest {},{} \tvel {},{}",
            worker_state.name,
            worker_state.pos.x, worker_state.pos.y,
            worker_state.dest.unwrap().x, worker_state.dest.unwrap().y,
            worker_state.vel.x, worker_state.vel.y,);

        // Verify the mobility model did not consider this worker as newly arrived.
        assert!(newly_arrived_state.is_none());
        
        // Verify the worker did not move
        assert_eq!(worker_state.pos, test_worker.previous_position);

        // Update the destination and velocity, the mobility model sets
        // these new values once the pause time has elapsed.
        test_worker.destination = worker_state.dest.unwrap();
        test_worker.velocity = worker_state.vel;
    }
}
