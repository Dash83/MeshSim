// extern crate mesh_simulator;
// extern crate diesel;

use crate::logging::{self, *};
use crate::mobility2::*;
use chrono::prelude::*;

use diesel::prelude::*;
use diesel::sql_query;

use slog::Logger;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;

/***********************************************/
/**************** Shared Resources *************/
/***********************************************/
lazy_static! {
    pub static ref WIRELESS_NIC: Mutex<()> = Mutex::new(());
}

embed_migrations!("migrations");

/***********************************************/
/***************  Test Data Types  *************/
/***********************************************/
pub type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct TestSetup {
    pub test_name: String,
    pub test_file: String,
    pub db_name: String,
    pub work_dir: String,
    pub db_env_file: Option<String>,
    pub worker: String,
    pub master: String,
    pub logger: Logger,
}

/*******************************************
*********** Utility functions *************
********************************************/
pub fn get_tests_root() -> String {
    // use std::env;
    let test_home = env::var("MESHSIM_TEST_DIR").unwrap_or(String::from("/tmp/"));
    test_home
}

pub fn create_test_dir<'a>(test_name: &'a str) -> String {
    // let now: DateTime<Utc> = Utc::now();
    // let test_dir_path = format!("{}{}_{}", &get_tests_root(), test_name, now.timestamp());
    let test_dir_path = format!("{}{}", &get_tests_root(), test_name);
    let test_dir = Path::new(&test_dir_path);

    if !test_dir.exists() {
        fs::create_dir(&test_dir_path).expect(&format!(
            "Unable to create test results directory {}",
            test_dir_path
        ));
    }

    test_dir_path.clone()
}

pub fn get_test_specification_dir() -> PathBuf {
    env::current_dir()
        .expect("Couldn't get current dir")
        .join("tests")
        .join("integration")
        .join("specs")
}

pub fn get_master_path() -> String {
    let file_pb = env::current_dir()
        .expect("Couldn't get current dir")
        .join("target")
        .join("debug")
        .join("master_cli");
    format!("{}", file_pb.display())
}

pub fn get_worker_path() -> String {
    let file_pb = env::current_dir()
        .expect("Couldn't get current dir")
        .join("target")
        .join("debug")
        .join("worker_cli");
    format!("{}", file_pb.display())
}

pub fn get_test_path<'a>(test: &'a str) -> String {
    let file_pb = get_test_specification_dir().join(test);
    format!("{}", file_pb.display())
}

pub fn setup(base_name: &str, log_to_term: bool, create_db: bool) -> TestSetup {
    let db_name = format!("{}_{}", base_name.to_lowercase(), Utc::now().timestamp());
    let work_dir = create_test_dir(&db_name);
    let log_file = format!("{}{}{}.log", work_dir, std::path::MAIN_SEPARATOR, base_name);
    let logger = logging::create_logger(log_file, log_to_term).expect("Failed to create logger");
    let worker = get_worker_path();
    let master = get_master_path();
    let test_file = get_test_path(&format!("{}.toml", base_name));
    let db_env_file =
        create_env_file(&db_name, &work_dir, &logger).expect("Could not create env file");

    if create_db {
        let root_conn_parts = parse_env_file(ROOT_ENV_FILE).expect("Could not parse env file");
        let owner: String = root_conn_parts.user_pwd.split(':').collect::<Vec<&str>>()[0].into();
        let root_conn =
            get_db_connection(ROOT_ENV_FILE, &logger).expect("Could not connect to root DB");
        let _ = create_database(&root_conn, &db_name, &owner, &logger).expect("Could not crete DB");

        let exp_conn =
            get_db_connection(&db_env_file, &logger).expect("Could not connect to experiment DB");
        let _ = embedded_migrations::run(&exp_conn);
    }

    TestSetup {
        test_name: base_name.into(),
        test_file,
        db_name,
        work_dir,
        db_env_file: Some(db_env_file),
        worker,
        master,
        logger,
    }
}

pub fn teardown(data: TestSetup, delete_db: bool) {
    //Delete the created database if applicable.
    if data.db_env_file.is_some() && delete_db {
        let root_env_file = String::from(ROOT_ENV_FILE);
        let conn = get_db_connection(&root_env_file, &data.logger)
            .expect("Could not get DB connection for cleanup");
        let query_str = format!("DROP DATABASE {};", &data.db_name);

        let q = sql_query(query_str);
        //     .bind::<Text, _>(data.db_name);
        let debug_q = diesel::debug_query::<diesel::pg::Pg, _>(&q);
        debug!(&data.logger, "Query: {}", &debug_q);

        let _rows = q
            .execute(&conn)
            .expect("Could not execute DROP DATABASE statement");
    }

    //Remove the leftover logs and files in the work directory
    fs::remove_dir_all(&data.work_dir).expect("Failed to remove results directory");
}

//********************************
//**** RGR utility Functions *****
//********************************
pub fn count_data_packets(log_recors: &Vec<LogEntry>) -> usize {
    let mut packet_count = 0;
    for record in log_recors.iter() {
        if record.status.is_some() && record.msg_type.is_some() {
            let status = &record.status.clone().unwrap();
            let msg_type = &record.msg_type.clone().unwrap();
            if status == "ACCEPTED" && msg_type == "DATA" {
                packet_count += 1;
            }
        }
    }
    packet_count
}

// fn count_all_packets<P: AsRef<Path>>(pattern: &str, dir: &str) -> usize {
pub fn count_all_packets(pattern: &str, dir: &str) {
    let mut targets = PathBuf::from(dir);
    targets.push("node5.log");
    let targets = targets
        .to_str()
        .expect("Could not encode DIR argument to UTF-8");
    let ls_child = Command::new("ls")
        .arg(targets)
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to get list of files");
    let files = ls_child.stdout.expect("Failed to get output from ls");
    println!("Files: {:?}", &files);

    let grep_child = Command::new("grep")
        .arg("-i")
        .arg(pattern)
        .arg(targets)
        // .stdin(Stdio::from(files))
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to run grep process");
    let output = grep_child
        .wait_with_output()
        .expect("Failed to get grep output");
    let out_s = String::from_utf8(output.stdout).expect("Could not parse output into valid utf8");
    println!("Incoming messages:\n{}", out_s);
}
