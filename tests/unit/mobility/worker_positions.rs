extern crate mesh_simulator;

use diesel::PgConnection;
use mesh_simulator::mobility::Position;
use mesh_simulator::tests::common::*;
use mesh_simulator::backend::*;

use super::common::MobilityTestWorker;

#[test]
fn test_worker_positions() {
    // Setup the test environment and create a database
    let mut data = setup("worker_positions", false, true);

    // Get the postgresql connection
    let env_file = data.db_env_file.take()
        .expect("Failed to unwrap env_file");
    let conn = get_db_connection_by_file(env_file.clone(), &data.logger)
        .expect("Could not get DB connection");

    // Add a list of test workers to the mobility system
    let mut test_workers = MobilityTestWorker::setup(&data, &conn);

    // Test 1: update the worker's positions once and verify their new position
    test_update_worker_positions(&conn, test_workers.as_mut());

    // Test 2: continue to update the worker positions until they reach their destination
    test_update_to_destination(&conn, test_workers.as_mut());

    // Test 3: continue to update the worker positions after they reached their destination.
    // In the absence of a mobility model, the workers are expected to continue to move with the same velocity past their destination.
    test_update_past_destination(&conn, test_workers.as_mut());

    //Test passed. Results are not needed.
    teardown(data, true);
}

/// Updates the worker positions once and verifies their new position with respect to its previous position.
fn test_update_worker_positions(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>) {
    // Update all worker positions
    let _res = update_worker_positions(&conn)
        .expect("Failed to update worker positions.");

    verify_positions(&conn, test_workers);
}

/// Updates the worker positions until they reach their destination.
fn test_update_to_destination(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>) {
    let steps_to_destination = test_workers[0].get_steps_to_destination();

    for step in 1..steps_to_destination {
        println!("----- Iteration {}", step);

        // Update all worker positions
        let _res = update_worker_positions(&conn)
            .expect("Failed to update worker positions.");

        // Verify the worker's position with respect to its previous position.
        verify_positions(&conn, test_workers);
    }
}

fn test_update_past_destination(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>) {
    let steps_to_destination = test_workers[0].get_steps_to_destination();

    for step in steps_to_destination..(steps_to_destination + 3) {
        println!("----- Iteration {}", step);

        // Update all worker positions
        let _res = update_worker_positions(&conn)
            .expect("Failed to update worker positions.");

        // Verify the worker's position with respect to its previous position.
        verify_positions(&conn, test_workers);
    }
}

/// Sanity checks the current position of every worker before it reaches its destination
fn verify_positions(conn: &PgConnection, test_workers: &mut Vec<MobilityTestWorker>) {
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
        println!("Worker {} at {},{}", worker_state.name, worker_state.pos.x, worker_state.pos.y);
 
        // Before reaching its destination, the worker must keep its original velocity and destination regardless of the mobility model.
        // In the absence of a mobility model, the worker keeps the same values even after reaching its destination.
        assert!(worker_state.vel == test_worker.velocity);
        assert!(worker_state.dest.unwrap() == test_worker.destination);

        // Verify the worker moved to the expected position.
        // Note that in the absence of a mobility model, the worker continues
        // to move without pause after reaching its destination.
        let expected_position = Position {
            x: test_worker.previous_position.x + worker_state.vel.x,
            y: test_worker.previous_position.y + worker_state.vel.y
        };

        /*
        // If the worker were to reach a position that is near its destination,
        // where near means past its destination but closer than it will be on
        // the next iteration, it is expected to 'cling' to its exact
        // destination coordinates.
        let next_position = Position {
            x: expected_position.x + worker_state.vel.x,
            y: expected_position.y + worker_state.vel.y
        };
        let dist_to_prv = test_worker.destination.distance(&test_worker.previous_position);
        let dist_to_exp = test_worker.destination.distance(&expected_position);
        let dist_to_nxt = test_worker.destination.distance(&next_position);
        // TRUE if both previous and expected positions are less than one step
        // away from the destination, and the expected position is closer to the
        // destination than the next position will be.
        let is_near =
            dist_to_prv > 0.0 && dist_to_prv < worker_state.vel.magnitude() &&
            dist_to_exp > 0.0 && dist_to_exp < worker_state.vel.magnitude() &&
            dist_to_exp < dist_to_nxt;

        if  is_near {
            println!("Worker {} is near with expected {},{} abs vel {},{} dest {},{} vel mag {} dist-exp {}",
                test_worker.name, 
                expected_position.x, expected_position.y, 
                worker_state.vel.x.abs(), worker_state.vel.y.abs(), 
                test_worker.destination.x, test_worker.destination.y,
                worker_state.vel.magnitude(),
                test_worker.destination.distance(&expected_position));
            assert!(worker_state.pos == test_worker.destination);
        }
        else {
            assert!(worker_state.pos == expected_position);
        }
        */
        assert!(worker_state.pos == expected_position);

        // Save the current position as the previous position
        test_worker.previous_position = worker_state.pos;
    }
}
