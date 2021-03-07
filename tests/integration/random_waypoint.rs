extern crate mesh_simulator;

use super::super::*;

use mesh_simulator::tests::common::*;
// use mesh_simulator::master::test_specification::TestSpec;

#[test]
fn three_node_movement() {
    let test_name = String::from("random_waypoint");
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

    // let test_spec = TestSpec::parse_test_spec(&data.test_file).expect("Could not parse test file");
    let mobility_records = logging::get_mobility_records(&master_log_file)
        .expect("Could not load mobility records");

    // Check that every recorded position for node1 is different (e.g. it kept moving)
    let mut prev_pos = mobility_records["node1"]
        .iter()
        .find(|_x| true)
        .map(|(_ts, (pos, _vel))| *pos)
        .unwrap();
    
    for (_ts, (pos, _vel)) in mobility_records["node1"].iter().skip(1) {
        assert_ne!(pos, &prev_pos);
        prev_pos = *pos;
    }
    
    // Check that every recorded position for node2 is different (e.g. it kept moving)
    let mut prev_pos = mobility_records["node2"]
        .iter()
        .find(|_x| true)
        .map(|(_ts, (pos, _vel))| *pos)
        .unwrap();
    
    for (_ts, (pos, _vel)) in mobility_records["node2"].iter().skip(1) {
        assert_ne!(pos, &prev_pos);
        prev_pos = *pos;
    }

    // Check that every recorded position for node3 is different (e.g. it kept moving)
    let mut prev_pos = mobility_records["node3"]
        .iter()
        .find(|_x| true)
        .map(|(_ts, (pos, _vel))| *pos)
        .unwrap();
    
    for (_ts, (pos, _vel)) in mobility_records["node3"].iter().skip(1) {
        assert_ne!(pos, &prev_pos);
        prev_pos = *pos;
    }

    //Test passed. Results are not needed.
    teardown(data, true);
}
