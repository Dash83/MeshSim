//! Module that implements the mobility layer based on the Postgres RDBMS
//!
//!
extern crate chrono;
extern crate dotenv;
extern crate strfmt;

use crate::{MeshSimError, MeshSimErrorKind};
// use crate::worker::Peer;
use crate::worker::radio::RadioTypes;
use models::*;

use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{Double, Text};
use diesel::Connection;
use slog::Logger;
use strfmt::strfmt;

embed_migrations!("migrations");

pub mod models;
pub mod schema;

/// The mean of human walking speeds
pub const HUMAN_SPEED_MEAN: f64 = 1.462; //meters per second.
/// Standard deviation of human walking speeds
pub const HUMAN_SPEED_STD_DEV: f64 = 0.164;
const MAX_DBOPEN_RETRY: i32 = 25;
const MAX_WAIT_MULTIPLIER: i32 = 8;
const BASE_WAIT_TIME: u64 = 250; //µs
///Postgres connection file for root DB
pub const ROOT_ENV_FILE: &str = ".env_root";
const EXP_ENV_FILE: &str = ".env";
const DB_BASE_NAME: &str = "meshsim";
const DB_CONN_PREAMBLE: &str = "DATABASE_URL=postgres://";
const DB_CONN_ENV_VAR: &str = "DATABASE_URL";
const DB_DEFAULT_COLLATION: &str = "en_GB.UTF-8";
const CREATE_DB_SQL: &str = "
CREATE DATABASE {db_name}
    WITH 
    OWNER = {owner}
    ENCODING = 'UTF8'
    LC_COLLATE = '{lc_collate}'
    LC_CTYPE = '{lc_ctype}'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
";

///Struct to encapsule the 2D position of the worker
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Default, Copy)]
pub struct Position {
    /// X component
    pub x: f64,
    /// Y component
    pub y: f64,
}

///Struct to encapsule the velocity vector of a worker
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy, Default)]
pub struct Velocity {
    /// X component
    pub x: f64,
    /// Y component
    pub y: f64,
}
/// Struct used for parsing connection files
pub struct ConnectionParts {
    pub user_pwd: String,
    // password: String,
    pub host: String,
    pub db_name: String,
}

// /// Used with get_db_connection.
// pub enum db_type {
//     /// Should only be used with create_db_objects.
//     root,
//     /// All other simulation operations should be used with this.
//     experiment,
// }

//****************************************
//********** Exported functions **********
//****************************************
/// Returns a connection to the database
pub fn get_db_connection<P: AsRef<Path>>(
    env_file: P,
    logger: &Logger,
) -> Result<PgConnection, MeshSimError> {
    env::remove_var(DB_CONN_ENV_VAR);

    dotenv::from_filename(env_file).ok();
    let database_url = env::var(DB_CONN_ENV_VAR).map_err(|e| {
        let error_msg = String::from("Failed to read DATABASE_URL environment variable");
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
            cause: Some(Box::new(e)),
        }
    })?;
    debug!(logger, "DATABASE_URL={}", database_url);
    let conn = PgConnection::establish(&database_url).map_err(|e| {
        let error_msg = format!(
            "Could not establish a connection to the DB: {}",
            &database_url
        );
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
            cause: Some(Box::new(e)),
        }
    })?;
    Ok(conn)
}

pub fn create_database(
    conn: &PgConnection,
    db_name: &String,
    owner: &String,
    logger: &Logger,
) -> Result<(), MeshSimError> {
    //Insert the DB name into the CREATE_DB query
    let mut vars = HashMap::new();
    vars.insert("db_name".to_string(), db_name);
    vars.insert("owner".to_string(), owner);
    let collation = env::var("MESHSIM_DB_COLLATE").unwrap_or(DB_DEFAULT_COLLATION.to_string());
    vars.insert("lc_collate".to_string(), &collation);
    vars.insert("lc_ctype".to_string(), &collation);
    let qry = strfmt(CREATE_DB_SQL, &vars).map_err(|e| {
        let error_msg = String::from("Could not bind DB name");
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    //Create the experiment DB
    let _ = sql_query(qry).execute(conn).map_err(|e| {
        let error_msg = String::from("Could not create experiment DB");
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    Ok(())
}

pub fn create_env_file(
    db_name: &str,
    work_dir: &str,
    logger: &Logger,
) -> Result<String, MeshSimError> {
    let conn_parts = parse_env_file(ROOT_ENV_FILE)?;
    let env_content = format!(
        "{}{}@{}/{}",
        DB_CONN_PREAMBLE, &conn_parts.user_pwd, &conn_parts.host, db_name
    );

    let mut file_name = PathBuf::new();
    file_name.push(work_dir); //the parent directory
    file_name.push(EXP_ENV_FILE); //Add the file name;
    if !file_name.exists() {
        let mut file = File::create(&file_name).expect("Could not create new .env file");
        file.write_all(env_content.as_bytes())
            .expect("Could not write new .env file");
    } else {
        debug!(&logger, "Env file already existed");
    }

    let f = file_name.to_string_lossy().into_owned();

    Ok(f)
}

/// Creates all the required database objects for a simulation
pub fn create_db_objects(
    work_dir: &String,
    db_name: &String,
    logger: &Logger,
) -> Result<String, MeshSimError> {
    //Make sure we use a lowercase version of db_name all throughout.
    let dbm = db_name.to_lowercase();

    //Create experiment database
    let root_conn_parts = parse_env_file(ROOT_ENV_FILE)?;
    let owner: String = root_conn_parts.user_pwd.split(':').collect::<Vec<&str>>()[0].into();
    let root_conn = get_db_connection(ROOT_ENV_FILE, &logger)?;
    let _ = create_database(&root_conn, &dbm, &owner, &logger)?;
    debug!(logger, "Experiment database created: {}", &dbm);

    //Write the .env file for the experiment
    let fpath = create_env_file(&dbm, work_dir, &logger)?;
    debug!(logger, "Connection file created: {}", &fpath);

    //Run all the migrations on the experiment DB
    let exp_conn = get_db_connection(&fpath, &logger)?;
    let _ = embedded_migrations::run(&exp_conn);

    Ok(fpath)
}

/// Registers a newly created worker into the mobility system
pub fn register_worker(
    conn: &PgConnection,
    w_name: String,
    w_id: String,
    pos: Position,
    vel: Velocity,
    dest: &Option<Position>,
    sr_address: Option<String>,
    lr_address: Option<String>,
    logger: &Logger,
) -> Result<i32, MeshSimError> {
    //Insert new worker
    let wr = insert_worker(conn, w_name, w_id, sr_address, lr_address, logger)?;

    //Add its position
    let rows = update_worker_position(conn, pos, wr.id, logger)?;

    //Add its velocity
    let rows = update_worker_vel(conn, vel, wr.id, logger)?;
    //Optionally: add it's destination
    if let Some(d) = dest {
        let rows = update_worker_target(conn, *d, wr.id, logger)?;
    }

    Ok(wr.id)
}

/// Updates the worker's velocity
pub fn update_worker_vel(
    conn: &PgConnection,
    vel: Velocity,
    id: i32,
    _logger: &Logger,
) -> Result<usize, MeshSimError> {
    use schema::worker_velocities::{self, *};

    let new_vel = new_vel {
        worker_id: id,
        x: vel.x,
        y: vel.y,
    };
    let rows = diesel::insert_into(worker_velocities::table)
        .values(&new_vel)
        .on_conflict(worker_id)
        .do_update()
        .set(&new_vel)
        .execute(conn)
        .map_err(|e| {
            let error_msg = String::from("Failed to insert/update worker velocity");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    Ok(rows)
}

/// Create a new target position for a worker
pub fn update_worker_target(
    conn: &PgConnection,
    dest: Position,
    id: i32,
    logger: &Logger,
) -> Result<usize, MeshSimError> {
    use schema::worker_destinations::{self, *};

    let new_dest = new_dest {
        worker_id: id,
        x: dest.x,
        y: dest.y,
    };
    let rows = diesel::insert_into(worker_destinations::table)
        .values(&new_dest)
        .on_conflict(worker_id)
        .do_update()
        .set(&new_dest)
        .execute(conn)
        .map_err(|e| {
            let error_msg = String::from("Failed to insert/update worker destination");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    Ok(rows)
}

/// Function exported exclusively for the use of the Master module.
/// Returns ids of all workers that have reached their destination.
pub fn select_workers_that_arrived(
    conn: &PgConnection,
) -> Result<HashMap<i32, Position>, MeshSimError> {
    use schema::worker_destinations;
    use schema::worker_positions;
    use schema::worker_velocities;

    let source: Vec<(i32, f64, f64, f64, f64, f64, f64)> = worker_positions::table
        .inner_join(worker_destinations::table)
        .inner_join(worker_velocities::table)
        .select((
            worker_positions::worker_id,
            worker_positions::x,
            worker_positions::y,
            worker_destinations::x,
            worker_destinations::y,
            worker_velocities::x,
            worker_velocities::y,
        ))
        .get_results(conn)
        .map_err(|e| {
            let error_msg =
                String::from("Failed to read worker positions, destinations and velocities");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    let mut result = HashMap::new();
    for (w_id, pos_x, pos_y, dest_x, dest_y, vel_x, vel_y) in source {
        let vel_magnitude = (vel_x.powi(2) + vel_y.powi(2)).sqrt();
        let remaining_distance = euclidean_distance(pos_x, pos_y, dest_x, dest_y);
        let dist_threshold = vel_magnitude / 2.0;
        if remaining_distance <= dist_threshold {
            result.insert(w_id, Position { x: pos_x, y: pos_y });
        }
    }

    Ok(result)
}

/// Function exported exclusively for the use of the Master module.
/// Sets the velocity of the worker ids to zero.
pub fn stop_workers(
    conn: &PgConnection,
    w_ids: &[i32],
    _logger: &Logger,
) -> Result<usize, MeshSimError> {
    use schema::worker_velocities::dsl::*;

    let rows = diesel::update(worker_velocities.filter(worker_id.eq_any(w_ids)))
        .set((x.eq(0.0), y.eq(0.0)))
        .execute(conn)
        .map_err(|e| {
            let error_msg = format!("Failed to stop workers {:?}", w_ids);
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    Ok(rows)
}

/// Function exported exclusively for the use of the Master module.
/// Updates the positions of all registered nodes according to their
/// respective velocity vector. Update happens every 1 second.
pub fn update_worker_positions(conn: &PgConnection) -> Result<usize, MeshSimError> {
    use diesel::pg::upsert::excluded;
    use schema::worker_positions::{self, *};
    use schema::worker_velocities::{self, *};

    let new_positions: Vec<new_pos> = worker_positions::table
        .inner_join(worker_velocities::table)
        .select((
            worker_positions::worker_id,
            worker_positions::x + worker_velocities::x,
            worker_positions::y + worker_velocities::y,
        ))
        .load(conn)
        .map_err(|e| {
            let error_msg = String::from("Failed to select updated worker positions");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    let rows = diesel::insert_into(worker_positions::table)
        .values(&new_positions)
        .on_conflict(worker_positions::worker_id)
        .do_update()
        .set((
            worker_positions::x.eq(excluded(worker_positions::x)),
            worker_positions::y.eq(excluded(worker_positions::y)),
        ))
        .execute(conn)
        .map_err(|e| {
            let error_msg = String::from("Failed to insert updated worker positions");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    Ok(rows)
}

// /// Gets the current position and database id of a given worker
// pub fn get_worker_position(
//     conn: &PgConnection,
//     worker_id: &str,
//     _logger: &Logger,
// ) -> Result<(Position, i64), MeshSimError> {
//     unimplemented!()
// }

/// Returns all workers within RANGE meters of the current position of WORKER_ID
pub fn get_workers_in_range(
    conn: &PgConnection,
    worker_name: &str,
    range: f64,
    logger: &Logger,
) -> Result<Vec<worker_record>, MeshSimError> {
    let query_str = String::from(
        "
        SELECT
            workers.id, 	
            workers.worker_id,
            workers.Worker_Name, 
            workers.Short_Range_Address,
            workers.Long_Range_Address
        FROM worker_positions a
        LEFT JOIN worker_positions b ON a.worker_id <> b.worker_id
        JOIN workers on workers.ID = b.worker_id
        WHERE a.worker_id in (SELECT ID FROM workers WHERE worker_name = $1)
        AND distance(b.x, b.y, a.x, a.y) < $2
    ",
    );

    let q = sql_query(query_str)
        .bind::<Text, _>(worker_name)
        .bind::<Double, _>(range);
    // let debug_q = diesel::debug_query::<diesel::pg::Pg, _>(&q);
    // debug!(logger, "Query: {}", &debug_q);

    let rows: Vec<worker_record> = q.get_results(conn).map_err(|e| {
        let error_msg = String::from("Failed to get workers in range");
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    Ok(rows)
}

/// Calculate the euclidean distance between 2 points.
pub fn euclidean_distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    let xs = (x2 - x1).powf(2.0);
    let ys = (y2 - y1).powf(2.0);
    (xs + ys).sqrt()
}

// // TODO: Replace return type for a new WorkerSnapshot type
// /// Get the positions of all workers
// pub fn get_all_worker_positions(
//     conn: &PgConnection,
// ) -> Result<Vec<(i64, String, f64, f64)>, MeshSimError> {
//     unimplemented!()
// }

///Inserts a new active transmitter into the system
pub fn insert_active_transmitter(
    conn: &PgConnection,
    worker_name: &str,
    range: RadioTypes,
    _logger: &Logger,
) -> Result<usize, MeshSimError> {
    use schema::workers;

    let rows = match range {
        RadioTypes::ShortRange => {
            use schema::active_wifi_transmitters;
            use schema::active_wifi_transmitters::dsl::*;

            workers::table
                .filter(workers::worker_name.eq(worker_name))
                .select(workers::id)
                .insert_into(active_wifi_transmitters::table)
                .into_columns(worker_id)
                .on_conflict_do_nothing()
                .execute(conn)
                .map_err(|e| {
                    let error_msg = String::from("Failed to register as active wifi transmitter");
                    MeshSimError {
                        kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                        cause: Some(Box::new(e)),
                    }
                })?
        }
        RadioTypes::LongRange => {
            use schema::active_lora_transmitters;
            use schema::active_lora_transmitters::dsl::*;

            workers::table
                .filter(workers::worker_name.eq(worker_name))
                .select(workers::id)
                .insert_into(active_lora_transmitters::table)
                .into_columns(worker_id)
                .on_conflict_do_nothing()
                .execute(conn)
                .map_err(|e| {
                    let error_msg = String::from("Failed to register as active lora transmitter");
                    MeshSimError {
                        kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                        cause: Some(Box::new(e)),
                    }
                })?
        }
    };

    Ok(rows)
}

///Remove a node from the active-transmitter list
pub fn remove_active_transmitter(
    conn: &PgConnection,
    w_name: &str,
    range: RadioTypes,
    logger: &Logger,
) -> Result<usize, MeshSimError> {
    use diesel::delete;
    use schema::workers;

    let rows = match range {
        RadioTypes::ShortRange => {
            use schema::active_wifi_transmitters;

            delete(active_wifi_transmitters::table)
                .filter(
                    active_wifi_transmitters::worker_id.eq_any(
                        workers::table
                            .filter(workers::worker_name.eq(w_name))
                            .select(workers::id),
                    ),
                )
                .execute(conn)
                .map_err(|e| {
                    let error_msg = String::from("Failed to remove as active wifi transmitter");
                    MeshSimError {
                        kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                        cause: Some(Box::new(e)),
                    }
                })?
        }
        RadioTypes::LongRange => {
            use schema::active_lora_transmitters;

            delete(active_lora_transmitters::table)
                .filter(
                    active_lora_transmitters::worker_id.eq_any(
                        workers::table
                            .filter(workers::worker_name.eq(w_name))
                            .select(workers::id),
                    ),
                )
                .execute(conn)
                .map_err(|e| {
                    let error_msg = String::from("Failed to remove as active lora transmitter");
                    MeshSimError {
                        kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                        cause: Some(Box::new(e)),
                    }
                })?
        }
    };

    Ok(rows)
}

// /// Function to get the active transmitters that correspond to the radio-type provided
// pub fn get_active_transmitters_in_range(
//     conn : &PgConnection,
//     worker_name: &String,
//     r_type: RadioTypes,
//     range : f64,
//     _logger: &Logger,
// ) -> Result<Vec<f64>, MeshSimError> {
//     unimplemented!()
// }

/// Function to get the active transmitters that correspond to the radio-type provided
pub fn register_active_transmitter_if_free(
    conn: &PgConnection,
    worker_name: &String,
    r_type: RadioTypes,
    range: f64,
    logger: &Logger,
) -> Result<usize, MeshSimError> {
    let query_str = match r_type {
        RadioTypes::ShortRange => String::from(
            "
            INSERT INTO active_wifi_transmitters (worker_id)
            SELECT ID 
                FROM workers 
                WHERE Worker_Name = $1 AND
                    (SELECT COUNT(*)
                        FROM worker_positions a
                        JOIN workers on workers.ID = a.worker_id
                        LEFT JOIN worker_positions b ON a.worker_id <> b.worker_id
                        INNER JOIN active_wifi_transmitters c ON b.worker_id = c.worker_id
                        WHERE workers.Worker_Name = $2
                                    AND distance(b.x, b.y, a.x, a.y) <= $3) = 0
            ",
        ),
        RadioTypes::LongRange => String::from(
            "
            INSERT INTO active_lora_transmitters (worker_id)
            SELECT ID 
                FROM workers 
                WHERE Worker_Name = $1 AND
                    (SELECT COUNT(*)
                        FROM worker_positions a
                        JOIN workers on workers.ID = a.worker_id
                        LEFT JOIN worker_positions b ON a.worker_id <> b.worker_id
                        INNER JOIN active_lora_transmitters c ON b.worker_id = c.worker_id
                        WHERE workers.Worker_Name = $2
                                    AND distance(b.x, b.y, a.x, a.y) <= $3) = 0
            ",
        ),
    };

    let q = sql_query(query_str)
        .bind::<Text, _>(worker_name)
        .bind::<Text, _>(worker_name)
        .bind::<Double, _>(range);
    // let debug_q = diesel::debug_query::<diesel::pg::Pg, _>(&q);
    // debug!(logger, "Query: {}", &debug_q);

    let rows = q.execute(conn).map_err(|e| {
        let r: String = r_type.into();
        let error_msg = format!(
            "Failed to register worker as an active transmitter. Radio={}",
            r
        );
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    Ok(rows)
}

/// Set the velocity of all workers to 0
pub fn stop_all_workers(conn: &PgConnection) -> Result<usize, MeshSimError> {
    use schema::worker_velocities::dsl::*;

    let rows = diesel::update(worker_velocities)
        .set((x.eq(0.0), y.eq(0.0)))
        .execute(conn)
        .map_err(|e| {
            let error_msg = String::from("Failed to stop workers");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    Ok(rows)
}

pub fn parse_env_file<P: AsRef<Path>>(file_path: P) -> Result<ConnectionParts, MeshSimError> {
    let mut root_file_content = String::new();
    let mut root_file = File::open(file_path).map_err(|e| {
        let error_msg = String::from("Could not open connection file");
        MeshSimError {
            kind: MeshSimErrorKind::Configuration(error_msg),
            cause: Some(Box::new(e)),
        }
    })?;
    root_file
        .read_to_string(&mut root_file_content)
        // .expect("Could not read file");
        .map_err(|e| {
            let error_msg = String::from("Failed to read connection file");
            MeshSimError {
                kind: MeshSimErrorKind::Configuration(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    let conn_data = root_file_content.as_str().replace(DB_CONN_PREAMBLE, "");
    let parts = conn_data
        .split(|c| c == '/' || c == '@')
        .collect::<Vec<&str>>();
    //Make sure the file content was properly formed
    assert_eq!(parts.len(), 3);
    let user_pwd = parts[0].into();
    let host = parts[1].into();
    let db_name = parts[2].into();
    // let cp_parts =  user_pwd.split(|c| c == ':').collect::<Vec<&str>>();
    // assert!(cp_parts.len() > 0);
    // let user = cp_parts[0].into();
    // let password = if cp_parts.len() > 1 {
    //     cp_parts[1].into()
    // } else {
    //     String::from("")
    // };

    let cp = ConnectionParts {
        user_pwd,
        host,
        db_name,
    };

    Ok(cp)
}

//****************************************
//********** Internal functions **********
//****************************************
fn insert_worker(
    conn: &PgConnection,
    w_name: String,
    w_id: String,
    sr_address: Option<String>,
    lr_address: Option<String>,
    _logger: &Logger,
) -> Result<worker_record, MeshSimError> {
    use schema::workers;
    // use schema::workers::dsl::*;

    let new_worker = new_worker {
        worker_name: &w_name,
        worker_id: &w_id,
        short_range_address: sr_address,
        long_range_address: lr_address,
    };

    let record: worker_record = diesel::insert_into(workers::table)
        .values(&new_worker)
        .get_result(conn)
        // .expect("Failed to insert new worker");
        .map_err(|e| {
            let error_msg = String::from("Failed to insert new worker");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    Ok(record)
}

fn update_worker_position(
    conn: &PgConnection,
    pos: Position,
    id: i32,
    _logger: &Logger,
) -> Result<usize, MeshSimError> {
    use schema::worker_positions::{self, *};

    let new_pos = new_pos {
        worker_id: id,
        x: pos.x,
        y: pos.y,
    };
    let rows = diesel::insert_into(worker_positions::table)
        .values(&new_pos)
        .on_conflict(worker_id)
        .do_update()
        .set(&new_pos)
        .execute(conn)
        .map_err(|e| {
            let error_msg = String::from("Failed to insert/update worker position");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    Ok(rows)
}

#[cfg(test)]
mod tests {
    // extern crate tests;
    use crate::mobility2::*;
    use crate::tests::common::*;
    use std::fs;
    use std::sync::Mutex;
    use std::thread;
    use std::time::Duration;

    lazy_static! {
        /// Used to serialize some tests that will create conflicts if running in parallel.
        static ref DB: Mutex<()> = Mutex::new(());
    }

    pub const TEST_DB_ENV_FILE: &str = "TEST_DB_ENV_FILE";
    pub const TEST_DB_NAME: &str = "TEST_DB_NAME";

    #[test]
    fn test_conn_get_db_connection_root() {
        let test_data = setup("get_db_connection_root", false, false);

        // Connect to root DB. If nothing failed, the test was succesful.
        let root_env_file = String::from(ROOT_ENV_FILE);
        let _conn = get_db_connection(&root_env_file, &test_data.logger)
            .expect("Could not get DB connection");

        //Test passed. Results are not needed.
        teardown(test_data, false);
    }

    // #[test]
    // fn test_conn_2_get_db_connection_exp() {
    //     let work_dir = create_test_dir("get_db_connection_exp");
    //     let log_file = format!("{}{}{}", work_dir, std::path::MAIN_SEPARATOR, "test_get_db_connection_exp.log");
    //     let logger = logging::create_logger(log_file, false).expect("Failed to create logger");
    //     // Connect to root DB. If nothing failed, the test was succesful.
    //     let _conn = get_db_connection(db_type::experiment, &logger).expect("Could not get DB connection");

    //     //Test passed. Results are not needed.
    //     fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
    // }

    #[test]
    fn test_db_01_create_db_objects() {
        let _db = DB.lock().expect("Unable to acquire DB lock");
        // let _ = create_db_objects(&logger).expect("Could not create db objects");
        let mut data = setup("create_db_objects", false, true);

        //Connect to experiment DB
        //If nothing failed, the test was succesful.
        let env_file = data.db_env_file.take().unwrap();
        let conn =
            get_db_connection(&env_file, &data.logger).expect("Failed to connect to experiment DB");

        //TODO: Select db name and check it matches de DB_NAME pattern: SELECT current_database();
        // let query_str = String::from("SELECT oid FROM pg_database WHERE datname = $1;");

        // let q = sql_query(query_str)
        //     .bind::<Text, _>(data.db_name);
        // let debug_q = diesel::debug_query::<diesel::pg::Pg, _>(&q);
        // debug!(&data.logger, "Query: {}", &debug_q);

        // let _rows: Vec<String>  = q.get_results(&conn).expect("Could not execute select database from catalog");

        //Set an environment variable with the .env file that will be used in this test module
        env::set_var(TEST_DB_ENV_FILE, &env_file.clone());
        env::set_var(TEST_DB_NAME, &data.db_name.clone());

        //No teardown performed, as all other tests in this module depend on this test.

        //Do not delete anything, as the other tests depend on this test directory.
        // teardown(data, false);
    }

    #[test]
    fn test_db_02_register_new_workers() {
        use schema::workers::dsl::*;

        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(10));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("register_new_worker", false, false);
        data.db_env_file = Some(
            env::var(TEST_DB_ENV_FILE)
                .expect("Could not read TEST_DB_ENV_FILE environment variable"),
        );
        let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
            .expect("Failed to connect to experiment DB");

        let _ = register_worker(
            &conn,
            String::from("Worker1"),
            String::from("12312398982"),
            Position { x: 10.0, y: 10.0 },
            Velocity { x: 1.7, y: 2.1 },
            &Some(Position { x: 100.0, y: 100.0 }),
            Some(String::from("[::]:1234")),
            Some(String::from("[::]:2345")),
            &data.logger,
        )
        .expect("Failed to register Worker1");

        let _ = register_worker(
            &conn,
            String::from("Worker2"),
            String::from("9895"),
            Position { x: 20.0, y: 20.0 },
            Velocity { x: 1.7, y: 2.1 },
            &Some(Position { x: 100.0, y: 100.0 }),
            Some(String::from("[::]:2341")),
            Some(String::from("[::]:3452")),
            &data.logger,
        )
        .expect("Failed to register Worker2");

        let _ = register_worker(
            &conn,
            String::from("Worker3"),
            String::from("112233"),
            Position { x: 0.0, y: 0.0 },
            Velocity { x: 3.1, y: 0.1 },
            &Some(Position { x: 100.0, y: 100.0 }),
            Some(String::from("[::]:3412")),
            Some(String::from("[::]:4523")),
            &data.logger,
        )
        .expect("Failed to register Worker3");

        let _ = register_worker(
            &conn,
            String::from("Worker4"),
            String::from("5123"),
            Position { x: -10.0, y: -10.0 },
            Velocity { x: 2.0, y: 2.0 },
            &Some(Position { x: -8.0, y: -8.0 }),
            Some(String::from("[::]:4123")),
            Some(String::from("[::]:5234")),
            &data.logger,
        )
        .expect("Failed to register Worker4");

        let _ = register_worker(
            &conn,
            String::from("Worker5"),
            String::from("89500"),
            Position { x: 25.0, y: 25.0 },
            Velocity { x: 0.7, y: 1.7 },
            &Some(Position { x: 100.0, y: 100.0 }),
            Some(String::from("[::]:6789")),
            Some(String::from("[::]:7890")),
            &data.logger,
        )
        .expect("Failed to register Worker5");

        let source: Vec<i32> = workers
            .select(id)
            .get_results(&conn)
            .expect("Could not select workers");
        assert_eq!(source.len(), 5);

        //Test passed. Results are not needed.
        teardown(data, false);
    }

    #[test]
    fn test_db_03_update_worker_positions() {
        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(15));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("update_worker_positions", false, false);
        data.db_env_file = Some(
            env::var(TEST_DB_ENV_FILE)
                .expect("Could not read TEST_DB_ENV_FILE environment variable"),
        );
        let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
            .expect("Failed to connect to experiment DB");
        let positions = update_worker_positions(&conn).expect("Could not update all workers");
        debug!(&data.logger, "New Positions: {:?}", positions);

        //Test passed. Results are not needed.
        teardown(data, false);
    }

    #[test]
    fn test_db_04_insert_active_wifi_transmitter() {
        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(20));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("insert_active_wifi_transmitter", false, false);
        data.db_env_file = Some(
            env::var(TEST_DB_ENV_FILE)
                .expect("Could not read TEST_DB_ENV_FILE environment variable"),
        );
        let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
            .expect("Failed to connect to experiment DB");
        let rows = insert_active_transmitter(
            &conn,
            &String::from("Worker1"),
            RadioTypes::ShortRange,
            &data.logger,
        )
        .expect("Failed to register Worker1 as an active wifi transmitter");
        assert_eq!(rows, 1);

        //If called a second time, it should not affect any rows
        let rows = insert_active_transmitter(
            &conn,
            &String::from("Worker1"),
            RadioTypes::ShortRange,
            &data.logger,
        )
        .expect("Failed to register Worker1 as an active wifi transmitter");
        assert_eq!(rows, 0);

        //Test passed. Results are not needed.
        teardown(data, false);
    }

    #[test]
    fn test_db_05_insert_active_lora_transmitter() {
        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(25));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("insert_active_lora_transmitter", false, false);
        data.db_env_file = Some(
            env::var(TEST_DB_ENV_FILE)
                .expect("Could not read TEST_DB_ENV_FILE environment variable"),
        );
        let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
            .expect("Failed to connect to experiment DB");
        let rows = insert_active_transmitter(
            &conn,
            &String::from("Worker1"),
            RadioTypes::LongRange,
            &data.logger,
        )
        .expect("Failed to register Worker1 as an active wifi transmitter");
        assert_eq!(rows, 1);

        //If called a second time, it should not affect any rows
        let rows = insert_active_transmitter(
            &conn,
            &String::from("Worker1"),
            RadioTypes::LongRange,
            &data.logger,
        )
        .expect("Failed to register Worker1 as an active wifi transmitter");
        assert_eq!(rows, 0);

        //Test passed. Results are not needed.
        teardown(data, false);
    }

    #[test]
    fn test_db_06_register_if_wifi_free_fail() {
        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(30));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("register_if_free_fail", false, false);
        data.db_env_file = Some(
            env::var(TEST_DB_ENV_FILE)
                .expect("Could not read TEST_DB_ENV_FILE environment variable"),
        );
        let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
            .expect("Failed to connect to experiment DB");
        let rows = register_active_transmitter_if_free(
            &conn,
            &String::from("Worker2"),
            RadioTypes::ShortRange,
            50.0,
            &data.logger,
        )
        .expect("register_active_transmitter_if_free Failed");
        assert_eq!(rows, 0);

        //Test passed. Results are not needed.
        teardown(data, false);
    }

    #[test]
    fn test_db_07_remove_active_transmitter() {
        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(35));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("remove_active_transmitter", false, false);
        data.db_env_file = Some(
            env::var(TEST_DB_ENV_FILE)
                .expect("Could not read TEST_DB_ENV_FILE environment variable"),
        );
        let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
            .expect("Failed to connect to experiment DB");
        let rows = remove_active_transmitter(
            &conn,
            &String::from("Worker1"),
            RadioTypes::ShortRange,
            &data.logger,
        )
        .expect("Failed to remove Worker1 as an active wifi transmitter");
        assert_eq!(rows, 1);

        //If called a second time, it should not affect any rows
        let rows = remove_active_transmitter(
            &conn,
            &String::from("Worker1"),
            RadioTypes::ShortRange,
            &data.logger,
        )
        .expect("Failed to remove Worker1 as an active wifi transmitter");
        assert_eq!(rows, 0);

        //Test passed. Results are not needed.
        teardown(data, false);
    }

    #[test]
    fn test_db_08_register_if_wifi_free_ok() {
        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(40));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("register_if_wifi_free_ok", false, false);
        data.db_env_file = Some(
            env::var(TEST_DB_ENV_FILE)
                .expect("Could not read TEST_DB_ENV_FILE environment variable"),
        );
        let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
            .expect("Failed to connect to experiment DB");
        let rows = register_active_transmitter_if_free(
            &conn,
            &String::from("Worker2"),
            RadioTypes::ShortRange,
            50.0,
            &data.logger,
        )
        .expect("register_active_transmitter_if_free Failed");
        assert_eq!(rows, 1);

        //Test passed. Results are not needed.
        teardown(data, false);
    }

    #[test]
    fn test_db_09_get_workers_in_range() {
        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(45));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("get_workers_in_range", false, false);
        data.db_env_file = Some(
            env::var(TEST_DB_ENV_FILE)
                .expect("Could not read TEST_DB_ENV_FILE environment variable"),
        );
        let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
            .expect("Failed to connect to experiment DB");
        let rows = get_workers_in_range(&conn, "Worker2", 50.0, &data.logger)
            .expect("get_workers_in_range Failed");
        let names: Vec<&String> = rows.iter().map(|v| &v.worker_name).collect();
        assert_eq!(rows.len(), 4);

        //Test passed. Results are not needed.
        teardown(data, false);
    }

    #[test]
    fn test_db_10_stop_two_workers() {
        use schema::worker_velocities::dsl::*;
        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(50));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("stop_two_workers", false, false);
        data.db_env_file = Some(
            env::var(TEST_DB_ENV_FILE)
                .expect("Could not read TEST_DB_ENV_FILE environment variable"),
        );
        let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
            .expect("Failed to connect to experiment DB");
        let workers_to_stop = [1, 3];
        let rows = stop_workers(&conn, &workers_to_stop, &data.logger)
            .expect("get_workers_in_range Failed");
        assert_eq!(rows, 2);

        let stopped: Vec<i32> = worker_velocities
            .filter(x.eq(0.0))
            .filter(y.eq(0.0))
            .select(worker_id)
            .get_results(&conn)
            .expect("Could not load stopped workers");
        assert_eq!(&stopped, &workers_to_stop);

        //Test passed. Results are not needed.
        teardown(data, false);
    }

    #[test]
    fn test_db_11_destination_reached() {
        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(55));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("destination_reached", false, false);
        data.db_env_file = Some(
            env::var(TEST_DB_ENV_FILE)
                .expect("Could not read TEST_DB_ENV_FILE environment variable"),
        );
        let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
            .expect("Failed to connect to experiment DB");
        let rows = select_workers_that_arrived(&conn).expect("get_workers_in_range Failed");
        assert_eq!(rows.len(), 1);

        //Test passed. Results are not needed.
        teardown(data, false);
    }

    #[test]
    fn test_db_12_stop_all_workers() {
        //GROSS HACK: Use a thread::sleep and mutex to make sure the tests in this module are run sequentially
        thread::sleep(Duration::from_millis(60));
        let _db = DB.lock().expect("Unable to acquire DB lock");

        let mut data = setup("stop_all_workers", false, false);
        {
            data.db_env_file = Some(
                env::var(TEST_DB_ENV_FILE)
                    .expect("Could not read TEST_DB_ENV_FILE environment variable"),
            );
            data.db_name =
                env::var(TEST_DB_NAME).expect("Could not read TEST_DB_NAME environment variable");
            let conn = get_db_connection(&data.db_env_file.clone().unwrap(), &data.logger)
                .expect("Failed to connect to experiment DB");
            let rows = stop_all_workers(&conn).expect("get_workers_in_range Failed");
            assert_eq!(rows, 5);
        }

        // Test passed. Results are not needed.
        // Make sure conn has been dropped before attempting to drop the database
        teardown(data, true);
    }
}
