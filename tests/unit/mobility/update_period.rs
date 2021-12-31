extern crate mesh_simulator;

use std::{collections::HashMap, time::Duration, thread::sleep};

use chrono::{Utc};
use diesel::PgConnection;
use mesh_simulator::{tests::common::{setup, teardown}, backend::{get_db_connection_by_file, update_worker_positions, select_all_workers_state}, mobility::{DEFAULT_MOBILITY_PERIOD, MINIMUM_MOBILITY_PERIOD}};

use super::common::*;

#[test]
#[ignore]
/// This test determines the minimum allowable value for the mobility update
/// period configuration parameter.
/// If this fails, update MINIMUM_MOBILITY_PERIOD accordingly. 
/// The time it takes for the update_worker_positions() function to run to
/// completion must not be longer than the update period. This threshold is
/// ultimatey determined by the hardware as well as the database configuration.
/// This value is determined using 100 workers, which is representative of a
/// reasonably sized system.
fn test_update_period() {
    let worker_cnt = 100;
    let test_runs = 100;
    
    // Setup the test environment and create a database
    let mut data = setup("update_period", false, true);

    // Get the postgresql connection
    let env_file = data.db_env_file.take()
        .expect("Failed to unwrap env_file");
    let conn = get_db_connection_by_file(env_file.clone(), &data.logger)
        .expect("Could not get DB connection");

    // Add 100 workers to the mobility system.
    // The neighbours will have random positions and velocities.
    let worker_1 = add_worker_to_test(&data, &conn, 
        None,
        None,
        None, 
        None, 
        None,
        None,
    );
    let mut test_workers = worker_1.add_neighbours(&data, &conn, worker_cnt - 1);

    // Warm up with 30 test runs
    get_max_update_time(&conn, &mut test_workers, 30);

    // Get the maximum update time over multiple test runs
    let max = get_max_update_time(&conn, &mut test_workers, test_runs);
    assert!(max <= MINIMUM_MOBILITY_PERIOD,
        "ERROR: A call to update_worker_positions() for {} workers completed in {} nanoseconds but the value of MINIMUM_MOBILITY_PERIOD is {}. Consider updating MINIMUM_MOBILITY_PERIOD accordingly.",
        worker_cnt, max, MINIMUM_MOBILITY_PERIOD);

    // Test passed. Results are not needed.
    teardown(data, true);
}

/// Gets the maximum time elapsed during a call to update_worker_positions
/// over the given number of test runs.
fn get_max_update_time(conn: &PgConnection, test_workers: &mut HashMap<String, MobilityTestWorker>, cnt: u64) -> u64 {
    let mut results = Vec::new();
    for _i in 0..cnt {
        let elapsed_ns = measure_update_once(conn, test_workers);
        results.push(elapsed_ns);

        sleep(Duration::from_millis(100));
    }

    return *results.iter().max().unwrap();
}

/// Calculates the time elapsed during a single call to update_worker_positions
fn measure_update_once(conn: &PgConnection, test_workers: &mut HashMap<String, MobilityTestWorker>) -> u64 {
    let period = DEFAULT_MOBILITY_PERIOD;
    let ratio = mobility_test_period_to_ratio(period);

    let start = Utc::now();

    // Update all worker positions
    let _res = update_worker_positions(&conn, ratio)
        .expect("Failed to update worker positions.");

    let elapsed = Utc::now() - start;
    let elapsed_ns = elapsed.num_nanoseconds().unwrap() as u64;
    println!("Update time: {} ns", elapsed_ns);

    // Verify the worker's position with respect to its previous position.
    let worker_states = select_all_workers_state(&conn)
        .expect("Failed to select worker positions.");
    for (_name, test_worker) in test_workers.iter_mut() {
        // Find the worker's state by worker name
        let state = worker_states.iter().find(|&s|s.name.eq(&test_worker.name));
        assert!(state.is_some());
        let worker_state = state.unwrap();
    
        // Verify the worker's new position.
        let expected_position = test_worker.get_next_position(Some(period));
        assert_eq!(worker_state.pos, expected_position);

        // Save the current position as the previous position
        test_worker.previous_position = worker_state.pos;
    }

    return elapsed_ns;
}
