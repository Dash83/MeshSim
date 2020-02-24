//! Module that implements the mobility layer based on the Postgres RDBMS
//! 
//! 
extern crate dotenv;
extern crate chrono;
extern crate strfmt;

use crate::{MeshSimError, MeshSimErrorKind};
use crate::worker::Peer;
use crate::worker::radio::RadioTypes;

use std::env;
use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;

use slog::Logger;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::Connection;
use chrono::prelude::*;
use strfmt::strfmt;

embed_migrations!("migrations");

const ROOT_ENV_FILE: &str = ".env_root";
const EXP_ENV_FILE: &str = ".env";
const DB_BASE_NAME: &str = "meshsim";
const DB_CONN_PREAMBLE: &str = "DATABASE_URL=postgres://";
const CREATE_DB_SQL: &str = "
CREATE DATABASE {db_name}
    WITH 
    OWNER = marco
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.UTF-8'
    LC_CTYPE = 'en_US.UTF-8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
";

///Struct to encapsule the 2D position of the worker
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Default)]
pub struct Position {
    /// X component
    pub x: f64,
    /// Y component
    pub y: f64,
}

///Struct to encapsule the velocity vector of a worker
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Default)]
pub struct Velocity {
    /// X component
    pub x: f64,
    /// Y component
    pub y: f64,
}

/// Used with get_db_connection.
pub enum db_type {
    /// Should only be used with create_db_objects.
    root,
    /// All other simulation operations should be used with this.
    experiment,
}

//****************************************
//********** Exported functions **********
//****************************************
/// Returns a connection to the database
pub fn get_db_connection(db : db_type, _logger: &Logger) -> Result<PgConnection, MeshSimError> {
    // dotenv().ok();


    match db {
        db_type::root => { 
            dotenv::from_filename(ROOT_ENV_FILE).ok();
        },
        db_type::experiment => { 
            dotenv::from_filename(EXP_ENV_FILE).ok();
        },
    }
    
    let database_url = env::var("DATABASE_URL")
        .map_err(|e| {
            let error_msg = String::from("Failed to read DATABASE_URL environment variable");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    let conn = PgConnection::establish(&database_url)
        .map_err(|e| {
            let error_msg = String::from("Could not establish a connection to the DB");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    Ok(conn)
}

/// Creates all the required database objects for a simulation
pub fn create_db_objects(conn: &PgConnection, logger: &Logger) -> Result<(), MeshSimError> {
    //Create a unique name for this database
    let utc: DateTime<Utc> = Utc::now();
    let num = utc.timestamp_millis();
    let mut vars = HashMap::new();
    let db_name = format!("{}_{}", DB_BASE_NAME, num);
    info!(logger, "DB name for the experiment:{}", &db_name);

    //Insert the DB name into the CREATE_DB query
    vars.insert("db_name".to_string(), &db_name);
    let qry = strfmt(CREATE_DB_SQL, &vars)
        .map_err(|e| {
            let error_msg = String::from("Could not bind DB name");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    
    //Create the experiment DB
    let _ = sql_query(qry)
        .execute(conn)
        .map_err(|e| {
            let error_msg = String::from("Could not create experiment DB");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    //Write the .env file for the experiment
    let mut root_file_content = String::new();
    let mut root_file = File::open(ROOT_ENV_FILE).expect("Could not open .env file");
    root_file.read_to_string(&mut root_file_content).expect("Could not read file");

    let conn_data = root_file_content.as_str().replace(DB_CONN_PREAMBLE, "");
    let parts =  conn_data.split(|c| c == '/' || c == '@').collect::<Vec<&str>>();
    let env_content = format!("{}{}@{}/{}", DB_CONN_PREAMBLE, parts[0], parts[1], db_name);
    let mut file = File::create(EXP_ENV_FILE).expect("Could not create new .env file");
    file.write_all(env_content.as_bytes()).expect("Could not write new .env file");    

    //Run all the migrations
    let _ = embedded_migrations::run(conn);

    Ok(())
}

/// Registers a newly created worker into the mobility system
pub fn register_worker(
    conn: &PgConnection,
    worker_name: String,
    worker_id: &str,
    pos: &Position,
    vel: &Velocity,
    dest: &Option<Position>,
    sr_address: Option<String>,
    lr_address: Option<String>,
    logger: &Logger,
) -> Result<i64, MeshSimError> { 
    unimplemented!()
}

/// Updates the worker's velocity
pub fn update_worker_vel(
    conn: &PgConnection,
    vel: &Velocity,
    worker_id: i64,
    _logger: &Logger,
) -> Result<usize, MeshSimError> { 
    unimplemented!()
}

/// Function exported exclusively for the use of the Master module.
/// Updates the positions of all registered nodes according to their
/// respective velocity vector. Update happens every 1 second.
pub fn update_worker_positions(conn: &PgConnection) -> Result<usize, MeshSimError> {
    unimplemented!()
}

/// Function exported exclusively for the use of the Master module.
/// Returns ids of all workers that have reached their destination.
pub fn select_final_positions(conn: &PgConnection) -> Result<Vec<(i64, f64, f64)>, MeshSimError> {
    unimplemented!()
}

/// Function exported exclusively for the use of the Master module.
/// Sets the velocity of the worker ids to zero.
pub fn stop_workers(
    conn: &PgConnection,
    w_ids: &[(i64, f64, f64)],
    _logger: &Logger,
) -> Result<usize, MeshSimError> { 
    unimplemented!()
}

/// Gets the current position and database id of a given worker
pub fn get_worker_position(
    conn: &PgConnection,
    worker_id: &str,
    _logger: &Logger,
) -> Result<(Position, i64), MeshSimError> { 
    unimplemented!()
}

/// Returns all workers within RANGE meters of the current position of WORKER_ID
pub fn get_workers_in_range(
    conn: &PgConnection,
    worker_id: &str,
    range: f64,
    logger: &Logger,
) -> Result<Vec<Peer>, MeshSimError> { 
    unimplemented!()
}

/// Calculate the euclidean distance between 2 points.
pub fn euclidean_distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    let xs = (x2 - x1).powf(2.0);
    let ys = (y2 - y1).powf(2.0);
    (xs + ys).sqrt()
}

/// Create a new target position for a worker
pub fn update_worker_target(
    conn: &PgConnection,
    worker_id: i64,
    target_pos: Position,
    logger: &Logger,
) -> Result<(), MeshSimError> { 
    unimplemented!()
}

// TODO: Replace return type for a new WorkerSnapshot type
/// Get the positions of all workers
pub fn get_all_worker_positions(
    conn: &PgConnection,
) -> Result<Vec<(i64, String, f64, f64)>, MeshSimError> {
    unimplemented!()
}

///Inserts a new active transmitter into the system
pub fn insert_active_transmitter(
    conn : &PgConnection,
    worker_name : &str,
    range: RadioTypes,
    _logger: &Logger,
) -> Result<(), MeshSimError> { 
    unimplemented!()
}

///Remove a node from the active-transmitter list
pub fn remove_active_transmitter(
    conn : &PgConnection,
    worker_name : &str,
    range: RadioTypes,
    _logger: &Logger,
) -> Result<(), MeshSimError> { 
    unimplemented!()
}

/// Function to get the active transmitters that correspond to the radio-type provided
pub fn get_active_transmitters_in_range(
    conn : &PgConnection,
    worker_name: &String,
    r_type: RadioTypes,
    range : f64,
    _logger: &Logger,
) -> Result<Vec<f64>, MeshSimError> { 
    unimplemented!()
}

/// Function to get the active transmitters that correspond to the radio-type provided
pub fn register_active_transmitters_if_free(
    conn : &PgConnection,
    worker_name: &String,
    r_type: RadioTypes,
    range : f64,
    _logger: &Logger,
) -> Result<usize, MeshSimError> { 
    unimplemented!()
}

///Function to get workers in range IF the medium is free
pub fn get_nodes_in_range_if_free(
    conn : &PgConnection,
    worker_name: &String,
    r_type: RadioTypes,
    range : f64,
    _logger: &Logger,
) -> Result<Vec<Peer>, MeshSimError> { 
    unimplemented!()
}

/// Set the velocity of all workers to 0
pub fn stop_all_workers(
    conn: &PgConnection
) -> Result<usize, MeshSimError> { 
    unimplemented!()
}

//****************************************
//********** Internal functions **********
//****************************************
fn insert_worker(
    conn: &PgConnection,
    // name: &dyn ToSql,
    worker_id: &str,
    // sr: &dyn ToSql,
    // lr: &dyn ToSql,
    _logger: &Logger,
) -> Result<usize, MeshSimError> { 
    unimplemented!()
}

fn update_worker_position(
    conn: &PgConnection,
    // x: &dyn ToSql,
    // y: &dyn ToSql,
    worker_id: i64,
    _logger: &Logger,
) -> Result<usize, MeshSimError> {
    unimplemented!()
}

#[cfg(test)]
mod tests { 
    use crate::worker::mobility2::*;
    use crate::logging;
    use std::fs;
    use std::path::{Path, PathBuf};

    /*******************************************
     *********** Utility functions *************
     ********************************************/
     fn get_tests_root() -> String {
        use std::env;
        let test_home = env::var("MESHSIM_TEST_DIR").unwrap_or(String::from("/tmp/"));
        test_home
    }

    fn create_test_dir<'a>(test_name: &'a str) -> String {
        let now: DateTime<Utc> = Utc::now();
        let test_dir_path = format!("{}{}_{}", &get_tests_root(), test_name, now.timestamp());
        let test_dir = Path::new(&test_dir_path);

        if !test_dir.exists() {
            fs::create_dir(&test_dir_path).expect(&format!(
                "Unable to create test results directory {}",
                test_dir_path
            ));
        }

        test_dir_path.clone()
    }

    #[test]
    fn test_get_db_connection() {
        // let work_dir = create_test_dir("get_db_connection");
        let logger = logging::create_discard_logger();
        let conn = get_db_connection(db_type::root, &logger).expect("Could not get DB connection");
    }

    #[test]
    fn test_create_db_objects() {
        // let work_dir = create_test_dir("get_db_connection");
        let logger = logging::create_discard_logger();
        //Get connection to root DB
        let conn = get_db_connection(db_type::root, &logger)
            .expect("Failed to connect to root DB");
        //Create experiment DB
        let _ = create_db_objects(&conn, &logger).expect("Could not create db objects");
        //Connect to experiment DB
        //If nothing failed, the test was succesful.
        let conn = get_db_connection(db_type::experiment, &logger)
            .expect("Failed to connect to experiment DB");
    }
}