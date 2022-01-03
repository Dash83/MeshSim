extern crate mesh_simulator;

use core::time;
use std::time::Duration;

use diesel::PgConnection;
use mesh_simulator::mobility::*;
use mesh_simulator::tests::common::*;
use mesh_simulator::backend::*;

use super::common::*;

#[test]
/// Basic unit tests for the handle_iteration() method of the IncreasedMobility
/// mobility handler.
fn test_random_waypoint_basic() {
    // Run the test
    test_random_waypoint("random_waypoint_basic", mesh_simulator::mobility::DEFAULT_MOBILITY_PERIOD);
}

#[test]
/// Unit tests for the handle_iteration() method of the IncreasedMobility
/// mobility handler with a 2 second mobility period (see MOBILITY_PERIOD).
fn test_random_waypoint_period_1() {
    let period = Duration::from_secs(2).as_nanos() as u64;

    // Run the test with a 2 second update period
    test_random_waypoint("random_waypoint_period_1", period);
}

#[test]
/// Unit tests for the handle_iteration() method of the IncreasedMobility
/// mobility handler with a 0.5 second mobility period (see MOBILITY_PERIOD).
fn test_random_waypoint_period_2() {
    let period = Duration::from_millis(500).as_nanos() as u64;

    // Run the test with a .5 second update period
    test_random_waypoint("random_waypoint_period_2", period);
}

/// Runs the basic tests for increased mobility
fn test_random_waypoint(base_name: &str, period: u64) {
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
    let random_waypoint_handler = RandomWaypoint::new(
        HUMAN_SPEED_MEAN,
        HUMAN_SPEED_STD_DEV,
        43,
        MOBILITY_TEST_AREA,
        pause_time,
        conn2,
        data.logger.clone(),
        Some(period),
        ).expect("Failed to create random waypoint mobility handler");
    let mut mobility_handler: Box<dyn MobilityHandler> = Box::new(random_waypoint_handler);
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
    
        verify_workers_state(&conn, &mut test_workers, &newly_arrived, Some(period));
    }

    // The workers were paused as they each reached their destination.
    // Call update_worker_positions() while they are all paused and
    // verify_workers_state() will verify they don't move while paused.
    for _step in 0..3 {
        println!("----- On Pause");

        // Update all worker positions
        let _res = update_worker_positions(&conn, ratio)
            .expect("Failed to update worker positions.");
    
        // Pass control to mobility model to recalculate the worker positions
        let newly_arrived = mobility_handler.handle_iteration()
            .expect("Mobility iteration failed.");

        // Verify the workers didn't move
        verify_workers_state(&conn, &mut test_workers, &newly_arrived, Some(period));
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

        // In this update, the workers didn't move yet
        verify_workers_state(&conn, &mut test_workers, &newly_arrived, Some(period));

        // Update the destination and velocity in the test worker structures
        // with the values set by the mobility handler. 
        resume_mobility(&conn, &mut test_workers);
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
    
        verify_workers_state(&conn, &mut test_workers, &newly_arrived, Some(period));
    }

    //Test passed. Results are not needed.
    teardown(data, true);
}

/// Sanity checks the node state of every worker with respect to its
/// previous position. A worker's behavior before reaching its original
/// destination is the same regardless of the mobility model.
fn verify_workers_state(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>, newly_arrived: &Vec<NodeState>, period: Option<u64>) {
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
    
        // Verify whether this worker was correctly reported as newly
        // arrived at its destination.
        let newly_arrived_state = newly_arrived.iter().find(|&s|s.name.eq(&worker.name));
        worker.verify_newly_arrived_state(&state, newly_arrived_state, period);

        if worker.is_at_destination(&state.pos, period) {
            // Pause the worker as soon as it reaches its destination
            worker.paused = true;
        }

        // Save the current position as the previous position
        worker.previous_position = state.pos;
    }
}

/// Sanity checks the node state of a set of paused workers with respect to
/// their previous position.
/// Note that all workers in the test_workers vector are expected to be paused.
/// The workers must not move during a pause, regardless of the mobility model.
fn resume_mobility(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>) {
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

        // These sanity checks are meant for workers that are currently paused.
        // A failure would indicate a bug in the test itself and not the code.
        assert!(worker.paused,
            "Worker {} is not paused, why was resume_mobility() called?",
            worker.name);

        // Upon reaching its destination, the random waypoint mobility model
        // will set a new destination. Assert it is different from the
        // previous one.
        assert_ne!(state.dest.unwrap(), worker.destination,
            "Destination {},{} is not new for worker {}.",
            state.dest.unwrap().x, state.dest.unwrap().y,
            worker.name);
        // Assert the new destination is within the simulation area.
        worker.validate_new_destination(&state.dest.unwrap());

        // After a pause, the mobility model will set a new velocity.
        // Verify the velocity vector actually leads to the new destination.
        worker.validate_new_velocity(&state.vel, &state.dest.unwrap());

        // Unpause the workers and update their destination and velocity.
        worker.paused = false;
        worker.destination = state.dest.unwrap();
        worker.velocity = state.vel;
    }
}
