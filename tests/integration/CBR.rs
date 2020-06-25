extern crate mesh_simulator;

use super::super::*;
use mesh_simulator::tests::common::*;

#[test]
fn test_cbr_basic() {
    let test_name = String::from("CBR");
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
        "End_Test action: Finished. 3 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    let node3_log_file = &format!("{}/log/node3.log", &data.work_dir);
    let node3_log_records = logging::get_log_records_from_file(&node3_log_file).unwrap();
    let mut received_packets = 0;

    for record in node3_log_records.iter() {
        if let Some(status) = &record.status {
            if status == "ACCEPTED" {
                received_packets += 1;
            }
        }
    }

    //Since the CBR source is configured to transmit 3 packets per second for 5 seconds,
    //we expect to count 15 received packets.
    assert_eq!(received_packets, 15);

    //Test passed. Results are not needed.
    teardown(data, true);
}
