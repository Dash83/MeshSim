extern crate mesh_simulator;

use core::time;

use diesel::PgConnection;
use mesh_simulator::mobility::*;
use mesh_simulator::tests::common::*;
use mesh_simulator::backend::*;

use crate::unit::mobility::common::{MobilityTestWorker, MOBILITY_TEST_AREA};

#[test]
fn test_increased_mobility() {
    // Time in milliseconds that the workers will pause upon reaching their destination
    let pause_time = 10000;
    // Setup the test environment and create a database
    let mut data = setup("worker_positions_incrmob", false, true);

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
        data.logger.clone()
    );
    let mut mobility_handler: Box<dyn MobilityHandler> = Box::new(increased_mobility_handler);

    // Add a list of test workers to the mobility system
    let mut test_workers = MobilityTestWorker::setup(&data, &conn);

    // Test 1: update the worker's positions once and verify their new position
    test_increased_mobility_once(&conn, test_workers.as_mut(), &mut mobility_handler);

    // Test 2: continue to update the worker positions until they reach their destination
    test_increased_mobility_to_dest(&conn, test_workers.as_mut(), &mut mobility_handler);

    println!("----- Pausing");
    std::thread::sleep(time::Duration::from_millis(pause_time as u64));

    // TODO: Test after pause
    
    //Test passed. Results are not needed.
    teardown(data, true);
}

/// Updates the worker positions once and verifies their new position with respect to its previous position.
fn test_increased_mobility_once(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>, mobility_handler: &mut Box<dyn MobilityHandler>) {
    // Update all worker positions
    let _res = update_worker_positions(&conn)
        .expect("Failed to update worker positions.");

    // Pass control to mobility model to recalculate the worker positions
    let newly_arrived = mobility_handler.handle_iteration()
        .expect("Mobility iteration failed.");

    verify_positions(&conn, test_workers, &newly_arrived);
}

/// Updates the worker positions until they reach their destination.
fn test_increased_mobility_to_dest(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>, mobility_handler: &mut Box<dyn MobilityHandler>) {
    let steps_to_destination = test_workers[0].get_steps_to_destination();

    for step in 1..steps_to_destination {
        println!("----- Iteration {}", step);

        // Update all worker positions
        let _res = update_worker_positions(&conn)
            .expect("Failed to update worker positions.");

        // Pass control to mobility model to recalculate the worker positions
        let newly_arrived = mobility_handler.handle_iteration()
            .expect("Mobility iteration failed.");

        // Verify the worker's position with respect to its previous position.
        verify_positions(&conn, test_workers, &newly_arrived);
    }
}

/// Sanity checks the current position of every worker before it reaches its destination
fn verify_positions(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>, newly_arrived: &Vec<NodeState>) {
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
        println!("Worker {} at {},{} with vel {},{} and dest {},{}",
            worker_state.name,
            worker_state.pos.x, worker_state.pos.y,
            worker_state.vel.x, worker_state.vel.y,
            worker_state.dest.unwrap().x, worker_state.dest.unwrap().y);

        // Check if the worker arrived at its destination, according to the mobility model
        let state = newly_arrived.iter().find(|&s|s.name.eq(&test_worker.name));
        let is_at_destination = state.is_some();
        let is_before_destination = !is_at_destination && !test_worker.paused;
 
        // Verify the worker's destination and velocity
        if is_before_destination {
            // Before reaching its destination, the worker must keep its original destination and velocity regardless of the mobility model.
            assert_eq!(worker_state.dest.unwrap(), test_worker.destination);
            assert_eq!(worker_state.vel, test_worker.velocity);
        }
        else if is_at_destination {
            // Upon reaching its destination, the mobility model sets a new destination and velocity.
            assert_ne!(worker_state.dest.unwrap(), test_worker.destination);
            assert_eq!(worker_state.vel, Velocity{x: 0.0, y: 0.0});
        }

        // Verify the worker's new position
        let expected_position = Position {
            x: test_worker.previous_position.x + worker_state.vel.x,
            y: test_worker.previous_position.y + worker_state.vel.y
        };
        if is_before_destination {
            // Before reaching its destination, it must be at the expected position
            assert_eq!(worker_state.pos, expected_position);
        }
        else if is_at_destination {
            // The mobility model will report the node as being at the
            // destination if it is close enough, where 'close' means at a
            // distance smaller than one hop past the destination but not
            // before. 
            if test_worker.velocity.magnitude() > 0.0 {
                assert!(worker_state.pos.distance(&test_worker.destination) < test_worker.velocity.magnitude());
            } else {
                // Worker does not move because its velocity is zero, but it
                // was originally positioned at its destination.
                assert_eq!(worker_state.pos, test_worker.initial_position);
            }
        }
        else {
            // The worker is paused after it reaches its destination
            // Verify it doesn't move further.
            assert_eq!(worker_state.pos, test_worker.previous_position);
        }

        // Save the current position as the previous position
        test_worker.previous_position = worker_state.pos;

        if is_at_destination {
            // The worker gets paused upon reaching its destination
            assert_eq!(test_worker.paused, false);
            test_worker.paused = true;
        }
    }
}