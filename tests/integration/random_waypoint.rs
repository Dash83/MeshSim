use super::super::*;

#[test]
fn test_random_waypoint_basic() {
    let test = get_test_path("random_waypoint_test.toml");
    let work_dir = create_test_dir("rand_wp");

    let program = get_master_path();
    let worker = get_worker_path();


    println!("Running command: {} -t {} -w {} -d {}", &program, &test, &worker, &work_dir);

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&program])
    .with_args(&["-t",  &test, "-w", &worker, "-d", &work_dir])
    .succeeds()
    .and()
    .stdout()
    .contains("End_Test action: Finished. 5 processes terminated.")
    .unwrap();

    let master_log_file = &format!("{}/log/MeshMaster.log", &work_dir);
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();

    let node_3_arrived = logging::find_log_record("msg", "1 workers have reached their destinations", &master_log_records);

    assert!(node_3_arrived.is_some());
}