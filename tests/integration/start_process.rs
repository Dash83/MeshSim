extern crate mesh_simulator;

use std::collections::HashMap;
use std::iter::FromIterator;

use chrono::Utc;
use chrono::DateTime;

use mesh_simulator::{master::test_specification::TestSpec, logging::{LOG_DIR_NAME, DEFAULT_MASTER_LOG, self, LogEntry}};
use mesh_simulator::tests::common::*;

/// Base test name.
/// The test runs multiple times using this .toml specs file.
/// Reuse any existing .toml specs file with at least MIN_WORKER_COUNT workers.
const BASE_TEST_NAME: &str = "aodv_basic";
/// Minimum number of workers that this test expects to find in the
/// BASE_TEST_NAME .toml specs file.
const MIN_WORKER_COUNT: usize = 25;

#[test]
/// Integration test to verify that the master process consistently starts
/// worker processes in the same order.
fn test_start_process() {
    // Run the test once to get a baseline.
    println!("----- Run 1 -----");
    let base_start_sequence = run_master();
    let worker_count = base_start_sequence.len();
    
    // Run the same test multiple times and compare the order in which
    // the workers were started each time.
    for run in 2..5 {
        println!("----- Run {} -----", run);
        let start_sequence = run_master();
        assert_eq!(start_sequence.len(), worker_count,
            "Test started {} workers but {} were expected according to base run.",
            start_sequence.len(), worker_count);

        base_start_sequence
            .iter()
            .zip(&start_sequence)
            .for_each(|((ts1, w1), (ts2, w2))| {
                println!("Run 1: {} \tstarted at {} | Run {}: {} \tstarted at {}",
                w1, ts1, run, w2, ts2);
            });

        // Assert the Master started workers in the same sequence as in the
        // first run.
        // Compare both vectors by worker name and get the number of workers
        // that appear at the same position in both vectors.
        // The vectors are already sorted by timestamp. We expect the worker
        // names to appear in the same order in both vectors.
        let matching = &base_start_sequence
            .iter()
            .zip(&start_sequence)
            .filter(|&((_ts1, w1), (_ts2, w2))| w1.eq(w2))
            .count();
        assert_eq!(matching, &worker_count,
            "The Master did not start workers in the same order in test runs 1 and {}. The start sequence matched for {} of {} workers.",
            run, matching, worker_count);
    }
}

/// Runs the Master once and gets the timestamps at which the workers
/// were started by the Master.
/// Returns a vector of timestamp to worker name, sorted by timestamp.
fn run_master() -> Vec<(DateTime<Utc>, String)> {
    // Create the test data from the .toml file.
    let data = setup(BASE_TEST_NAME, false, false);

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
    
    // Get the Master's log records 
    let master_log_records = logging::get_log_records_from_file(&master_log_file)
        .expect("Failed to get log records from Master");

    // Verify the master ran successfully by checking the number of worker
    // processes that were terminated at the end of the test.
    // Get the number of workers from the .toml file.
    let test_spec = TestSpec::parse_test_spec(&data.test_file).expect("Failed to parse test file.");
    let worker_count = test_spec.initial_nodes.len();
    assert!(worker_count >= MIN_WORKER_COUNT,
        "Specs file {}.toml has {} workers, but this test expects at least {} workers. Consider using a different .toml file for this test.",
        BASE_TEST_NAME, worker_count, MIN_WORKER_COUNT);
    verify_process_count(&master_log_records, worker_count);

    // Get the timestamp when each worker was started.
    let start_timestamps = get_start_timestamps(&master_log_records);
    assert_eq!(start_timestamps.len(), worker_count,
        "Timestamp vector contains {} workers, but test ran with {} workers.",
        start_timestamps.len(), worker_count);

    // Test passed. Results are not needed.
    teardown(data, true);

    return start_timestamps;
}

/// Verifies that the master ran to completion successfully by checking
/// the number of processes that were terminated at the end of the test.
fn verify_process_count(master_log_records: &Vec<LogEntry>, process_count: usize) {
    // Find this message among the master log records.
    let end_msg = format!("End_Test action: Finished. {} processes terminated.", process_count);
    let master_node_num = logging::find_record_by_msg(
        &end_msg,
        &master_log_records,
    );
    assert!(master_node_num.is_some(),
        "Master did not complete with the expected number of processes: {}", process_count);
}

/// Given the Master's log records, builds a vector of timestamp to worker
/// name, sorted by timestamp at which the Master started each worker.
fn get_start_timestamps(master_log_records: &Vec<LogEntry>) -> Vec<(DateTime<Utc>, String)> {
    let mut map: HashMap<DateTime<Utc>, String> = HashMap::new();

    // For each log record with a message like "Worker process node1 started"
    for entry in master_log_records
        .iter()
        .filter(|&m| m.msg.starts_with("Worker process ")) {
        // Extract the worker name
        let parts: Vec<&str> = entry.msg.split_whitespace().collect();
        let worker = parts[2].to_string();
        // Parse the timestamp
        let ts = entry.ts.parse::<DateTime<Utc>>()
            .expect("Failed to parse string into DateTime");

        map.insert(ts, worker);
    }

    // Convert to a vector to sort by timestamp
    let mut start_timestamps = Vec::from_iter(map);
    start_timestamps.sort();

    return start_timestamps;
}