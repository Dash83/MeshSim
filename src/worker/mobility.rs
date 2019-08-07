//! This module handles the position and mobility features of the worker
//!

use crate::worker::Peer;
use crate::{MeshSimError, MeshSimErrorKind};
use rand::RngCore;
use rusqlite::types::ToSql;
use rusqlite::{Connection, OpenFlags, NO_PARAMS};
use slog::Logger;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

/// Name used for the worker positions DB
pub const DB_NAME: &str = "worker_positions.db";
/// The mean of human walking speeds
pub const HUMAN_SPEED_MEAN: f64 = 1.462; //meters per second.
/// Standard deviation of human walking speeds
pub const HUMAN_SPEED_STD_DEV: f64 = 0.164;
const MAX_DBOPEN_RETRY: i32 = 11;

//region Queries
const CREATE_WORKERS_TBL_QRY : &str = "CREATE TABLE IF NOT EXISTS workers (
                                                    ID	                    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                                                    Worker_Name	            TEXT NOT NULL,
                                                    Worker_ID	            TEXT NOT NULL UNIQUE,
                                                    Short_Range_Address	    TEXT,
                                                    Long_Range_Address	    TEXT
                                            )";
const CREATE_WORKER_POS_TBL_QRY: &str = "CREATE TABLE IF NOT EXISTS  `worker_positions` (
                                                    `worker_id`	INTEGER UNIQUE,
                                                    X	REAL NOT NULL,
                                                    Y  REAL NOT NULL,
                                                    FOREIGN KEY(worker_id) REFERENCES workers(ID)
                                                );";

const CREATE_WORKER_VEL_TBL_QRY: &str = "CREATE TABLE IF NOT EXISTS  `worker_velocities` (
                                                    `worker_id`	INTEGER UNIQUE NOT NULL,
                                                    vel_x	REAL NOT NULL,
                                                    vel_y  REAL NOT NULL,
                                                    FOREIGN KEY(worker_id) REFERENCES workers(ID)
                                                );";
const CREATE_WORKER_DEST_TBL_QRY : &str = "CREATE TABLE IF NOT EXISTS  `worker_destinations` (
                                                    `worker_id`	INTEGER UNIQUE NOT NULL,
                                                    `dest_x`	INTEGER NOT NULL,
                                                    `dest_y`	INTEGER NOT NULL,
                                                    FOREIGN KEY(`worker_id`) REFERENCES `workers`(`ID`)
                                                );";

const INSERT_WORKER_QRY: &str =
    "INSERT INTO workers (Worker_Name, Worker_ID, Short_Range_Address, Long_Range_Address)
                                          VALUES (?1, ?2, ?3, ?4)";
const UPDATE_WORKER_POS_QRY: &str =
    "INSERT OR REPLACE INTO worker_positions (worker_id, x, y) VALUES (?1, ?2, ?3)";
const UPDATE_WORKER_VEL_QRY: &str =
    "INSERT OR REPLACE INTO worker_velocities (worker_id, vel_x, vel_y) VALUES (?1, ?2, ?3)";
const UPDATE_WORKER_DEST_QRY: &str =
    "INSERT OR REPLACE INTO worker_destinations (worker_id, dest_x, dest_y) VALUES (?1, ?2, ?3)";
const GET_WORKER_QRY : &str = "SELECT ID, Worker_Name, Worker_ID, Short_Range_Address, Long_Range_Address FROM Workers WHERE Worker_ID = (?)";
//const SELECT_OTHER_WORKERS_QRY : &str = "SELECT Worker_Name, Worker_ID, Short_Range_Address, Long_Range_Address FROM Workers WHERE Worker_ID != (?)";
const GET_WORKER_POS_QRY : &str =  "SELECT  Workers.ID,
                                                    Worker_positions.X,
                                                    Worker_positions.Y
                                            FROM Workers
                                            JOIN Worker_positions on Workers.ID = Worker_positions.worker_id
                                            WHERE Workers.Worker_ID = (?)";
const SELECT_OTHER_WORKERS_POS_QRY : &str = "SELECT Workers.worker_name,
		                                                    Workers.Worker_ID,
                                                            Workers.short_range_address, 
                                                            Workers.long_range_address, 
                                                            Worker_positions.X,
                                                            Worker_positions.Y
                                                    FROM Workers
                                                    JOIN Worker_positions on Workers.ID = Worker_positions.worker_id
                                                    WHERE Workers.Worker_ID != (?)";
const SELECT_ALL_WORKERS_POS_QRY : &str = "SELECT   Workers.ID,
                                                            Workers.worker_name,
                                                            Worker_positions.X,
                                                            Worker_positions.Y
                                                    FROM Workers
                                                    JOIN Worker_positions on Workers.ID = Worker_positions.worker_id";
//const SELECT_WORKERS_VEL_QRY : &str = "SELECT ID, vel_x, vel_y FROM worker_velocities";
const UPDATE_WORKER_POSITIONS_QRY : &str = "
                                            INSERT OR REPLACE INTO worker_positions
                                            (worker_id, x, y)
                                            select worker_positions.worker_id,
                                                    worker_positions.X + worker_velocities.vel_x,
                                                    worker_positions.Y + worker_velocities.vel_y
                                            from worker_velocities
                                            join worker_positions on worker_positions.worker_id = worker_velocities.worker_id;";
const STOP_WORKERS_QRY: &str = "
                                UPDATE worker_velocities
                                SET  vel_x = 0,
                                        vel_y = 0
                                WHERE worker_velocities.worker_id in ";
const SELECT_REMAINING_DIST_QRY : &str = "
                                select workers.ID,
                                        worker_destinations.dest_x,
                                        worker_positions.X,
                                        worker_destinations.dest_y,
                                        worker_positions.Y
                                from workers
                                join worker_positions on worker_positions.worker_id = workers.ID
                                join worker_destinations on worker_destinations.worker_id = workers.ID
                                order by workers.Worker_Name;";
const SET_WAL_MODE: &str = "PRAGMA journal_mode=WAL;";
const WAL_MODE_QRY: &str = "PRAGMA journal_mode;";
//const SET_TMP_MODE : &'static str = "PRAGMA temp_store=2"; //Instruct the database to keep temp tables in memory
//endregion Queries

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

/// Get a connection to the current DB
pub fn get_db_connection(path: &str, logger: &Logger) -> Result<Connection, MeshSimError> {
    let mut rng = rand::thread_rng();
    let mut conn: Option<Connection> = None;

    let mut dir = PathBuf::from(path);
    dir.push(DB_NAME);

    for _i in 0..MAX_DBOPEN_RETRY {
        match rusqlite::Connection::open_with_flags(
            &dir,
            OpenFlags::SQLITE_OPEN_SHARED_CACHE
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE,
        ) {
            Ok(c) => {
                conn = Some(c);
            }
            Err(e) => {
                warn!(logger, "Failed to establish DB connection: {}", e);
                let wait_time = rng.next_u64() % 100;
                let wait_dur = Duration::from_millis(wait_time);
                thread::sleep(wait_dur);
            }
        }

        if let Some(c) = conn {
            //Add busy handler
            c.busy_handler(Some(busy_callback)).map_err(|e| {
                let error_msg = String::from("Failed to set busy handler");
                MeshSimError {
                    kind: MeshSimErrorKind::ConnectionFailure(error_msg),
                    cause: Some(Box::new(e)),
                }
            })?;
            //Add User-defined functions
            c.create_scalar_function("distance", 4, true, move |ctx| {
                assert_eq!(ctx.len(), 4);

                let x1 = ctx.get::<f64>(0)?;
                let y1 = ctx.get::<f64>(1)?;
                let x2 = ctx.get::<f64>(2)?;
                let y2 = ctx.get::<f64>(3)?;
                let d = euclidean_distance(x1, y1, x2, y2);
                Ok(d)
            })
            .map_err(|e| {
                let error_msg = String::from("Failed to create SQL function");
                MeshSimError {
                    kind: MeshSimErrorKind::ConnectionFailure(error_msg),
                    cause: Some(Box::new(e)),
                }
            })?;
            return Ok(c);
        }
    }
    error!(logger, "Could not establish a DB connection");
    let error_msg = String::from("Exhausted retries to connect to the DB");
    Err(MeshSimError {
        kind: MeshSimErrorKind::ConnectionFailure(error_msg),
        cause: None,
    })
}

/// Crate the database tables
pub fn create_db_objects(conn: &Connection, logger: &Logger) -> Result<usize, MeshSimError> {
    //Set the DB journaling mode to WAL first.
    set_wal_mode(conn, logger)?;

    //Set the temp store mode
    //    let _ = set_tmp_mode(conn, logger)?;

    //Create workers table
    let _rows = conn
        .execute(CREATE_WORKERS_TBL_QRY, NO_PARAMS)
        .map_err(|e| {
            let error_msg = String::from("Failed to create Workers table");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    //Create positions table
    let _rows = conn
        .execute(CREATE_WORKER_POS_TBL_QRY, NO_PARAMS)
        .map_err(|e| {
            let error_msg = String::from("Failed to create Worker_Pos table");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    //Create velocities table
    let _rows = conn
        .execute(CREATE_WORKER_VEL_TBL_QRY, NO_PARAMS)
        .map_err(|e| {
            let error_msg = String::from("Failed to create Worker_Vel table");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    //Create destinations table
    let rows = conn
        .execute(CREATE_WORKER_DEST_TBL_QRY, NO_PARAMS)
        .map_err(|e| {
            let error_msg = String::from("Failed to create Worker_Dest table");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    Ok(rows)
}

/// Registers a newly created worker into the mobility system
pub fn register_worker(
    conn: &Connection,
    worker_name: String,
    worker_id: &str,
    pos: &Position,
    vel: &Velocity,
    dest: &Option<Position>,
    sr_address: Option<String>,
    lr_address: Option<String>,
    logger: &Logger,
) -> Result<i64, MeshSimError> {
    let name: &dyn ToSql = &worker_name;
    let x: &dyn ToSql = &pos.x;
    let y: &dyn ToSql = &pos.y;
    let sr: &dyn ToSql = &sr_address;
    let lr: &dyn ToSql = &lr_address;

    //Insert worker
    let _res = insert_worker(&conn, name, &worker_id, sr, lr, logger)?;
    // info!(&logger, "[DEBUG] worker inserted");

    //Get the ID that was generated for the worker
    let db_id: i64 = conn
        .prepare(GET_WORKER_QRY)
        .and_then(|mut stmt| stmt.query_row(&[&worker_id], |row| row.get(0)))
        .map_err(|e| {
            let error_msg = String::from("Could not get worker_id");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    //Insert position
    let _res = update_worker_position(&conn, x, y, db_id, logger).map_err(|e| {
        let error_msg = String::from("Failed to update worker position");
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    //Insert velocity
    let _res = update_worker_vel(&conn, vel, db_id, logger).map_err(|e| {
        let error_msg = String::from("Failed to update worker velocity");
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    //Insert destination if any
    if let Some(d) = dest {
        update_worker_target(&conn, db_id, d.clone(), logger).map_err(|e| {
            let error_msg = String::from("Failed to update worker destination");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(error_msg),
                cause: Some(Box::new(e)),
            }
        })?;
    }

    Ok(db_id)
}

//fn insert_worker(conn : &Connection,
//                 name : &dyn ToSql,
//                 worker_id : &str,
//                 sr : &dyn ToSql,
//                 lr : &dyn ToSql,
//                 logger : &Logger ) -> Result<usize, MeshSimError> {
//    let mut rows = 0;
//    let rng = rand::thread_rng();
//    let wid : &dyn ToSql = &worker_id;
//
//    for i in 0..MAX_DBOPEN_RETRY {
//        rows = match conn.execute( INSERT_WORKER_QRY, &[name, wid, sr, lr] ) {
//            Ok(r) => r,
//            Err(e) => {
//                error!(logger, "Failed to register worker: {}", e);
//                let wait_time = rng.next_u64() % 100;
//                info!(logger, "Will retry in {} ms", wait_time);
//                let wait_dur = Duration::from_millis(wait_time);
//                thread::sleep(wait_dur);
//                0
//            }
//        };
//
//        if rows > 0 {
//            //Worker registered
//            info!(logger, "Worker registered in DB successfully");
//            break;
//        } else if i+1 == MAX_DBOPEN_RETRY {
//            let err_msg = format!("Could not register worker {} in the DB", worker_id);
//            let error = MeshSimError{
//                kind : MeshSimErrorKind::SQLExecutionFailure(err_msg),
//                cause : None
//            };
//            Err(error)
//        }
//    }
//    Ok(rows)
//}

fn insert_worker(
    conn: &Connection,
    name: &dyn ToSql,
    worker_id: &str,
    sr: &dyn ToSql,
    lr: &dyn ToSql,
    _logger: &Logger,
) -> Result<usize, MeshSimError> {
    let wid: &dyn ToSql = &worker_id;

    let rows = conn
        .execute(INSERT_WORKER_QRY, &[name, wid, sr, lr])
        .map_err(|_| {
            let err_msg = format!("Failed to register worker {}", worker_id);
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: None,
            }
        })?;

    Ok(rows)
}

//fn update_worker_position(conn : &Connection,
//                     x : &dyn ToSql,
//                     y : &dyn ToSql,
//                     worker_id : i64,
//                     logger : &Logger ) -> Result<usize, MeshSimError> {
//    let mut rows = 0;
//    let wid : &dyn ToSql = &worker_id;
//    let rng = rand::thread_rng();
//
//    for i in 0..MAX_DBOPEN_RETRY {
//        rows = match conn.execute( UPDATE_WORKER_POS_QRY, &[wid, x, y] ) {
//            Ok(r) => r,
//            Err(e) => {
//                error!(logger, "Failed to update the position of worker: {}", e);
//                let wait_time = rng.next_u64() % 100;
//                info!(logger, "Will retry in {} ms", wait_time);
//                let wait_dur = Duration::from_millis(wait_time);
//                thread::sleep(wait_dur);
//                0
//            }
//        };
//
//        if rows > 0 {
//            //Worker registered
//            //debug!("Position updated!");
//            break;
//        } else if i+1 == MAX_DBOPEN_RETRY {
//            let err_msg = format!("Could not update the position of worker {}", worker_id);
//            let error = MeshSimError{
//                kind : MeshSimErrorKind::SQLExecutionFailure(err_msg),
//                cause : None
//            };
//            Err(error)
//        }
//    }
//
//    Ok(rows)
//}

fn update_worker_position(
    conn: &Connection,
    x: &dyn ToSql,
    y: &dyn ToSql,
    worker_id: i64,
    _logger: &Logger,
) -> Result<usize, MeshSimError> {
    let wid: &dyn ToSql = &worker_id;

    let rows = conn
        .execute(UPDATE_WORKER_POS_QRY, &[wid, x, y])
        .map_err(|_| {
            let err_msg = format!("Failed to update the position of worker {}", worker_id);
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: None,
            }
        })?;

    Ok(rows)
}

///// Updates the worker's velocity
//pub fn update_worker_vel(conn : &Connection,
//                     vel : &Velocity,
//                     worker_id : i64,
//                     logger : &Logger ) -> Result<usize, MeshSimError> {
//    let vel_x : &dyn ToSql = &vel.x;
//    let vel_y : &dyn ToSql = &vel.y;
//    let rows = 0;
//    let wid : &dyn ToSql = &worker_id;
//    let rng = rand::thread_rng();
//
//    for i in 0..MAX_DBOPEN_RETRY {
//        rows = match conn.execute( UPDATE_WORKER_VEL_QRY, &[wid, vel_x, vel_y] ) {
//            Ok(r) => r,
//            Err(e) => {
//                error!(logger, "Failed to update the velocity of worker: {}", e);
//                let wait_time = rng.next_u64() % 100;
//                info!(logger, "Will retry in {} ms", wait_time);
//                let wait_dur = Duration::from_millis(wait_time);
//                thread::sleep(wait_dur);
//                0
//    }
//        };
//
//        if rows > 0 {
//            //Worker registered
//            //debug!("Position updated!");
//            break;
//        } else if i+1 == MAX_DBOPEN_RETRY {
//            return Err(WorkerError::Sync(format!("Could not update the velocity of worker {}", worker_id)));
//        }
//    }
//
//    Ok(rows)
//}

/// Updates the worker's velocity
pub fn update_worker_vel(
    conn: &Connection,
    vel: &Velocity,
    worker_id: i64,
    _logger: &Logger,
) -> Result<usize, MeshSimError> {
    let vel_x: &dyn ToSql = &vel.x;
    let vel_y: &dyn ToSql = &vel.y;
    let wid: &dyn ToSql = &worker_id;

    let rows = conn
        .execute(UPDATE_WORKER_VEL_QRY, &[wid, vel_x, vel_y])
        .map_err(|_| {
            let err_msg = format!("Failed to update the velocity of worker {}", worker_id);
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: None,
            }
        })?;

    Ok(rows)
}

fn set_wal_mode(conn: &Connection, logger: &Logger) -> Result<(), MeshSimError> {
    let journal_mode: String = conn
        .prepare(WAL_MODE_QRY)
        .and_then(|mut stmt| stmt.query_row(NO_PARAMS, |row| row.get(0)))
        .map_err(|_| {
            let err_msg = String::from("Failed to read the DB's journal_mode");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: None,
            }
        })?;

    if journal_mode.to_uppercase() == "WAL" {
        info!(logger, "WAL mode already set");
        Ok(())
    } else {
        conn.prepare(SET_WAL_MODE)
            .and_then(|mut stmt| stmt.query_row(NO_PARAMS, |_| Ok(())))
            .map_err(|_| {
                let err_msg = String::from("Failed to set the DB's journal_mode to WAL");
                MeshSimError {
                    kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                    cause: None,
                }
            })?
    }
}

//fn set_tmp_mode(conn : &Connection, logger : &Logger) -> Result<(), WorkerError> {
//    let mut rng = rand::thread_rng();
//    let mut temp_store_mode = String::from("");
//    let mut stmt = conn.prepare(SET_TMP_MODE)?;
//
//    for _i in 0..MAX_DBOPEN_RETRY {
//        temp_store_mode = match stmt.query_row(NO_PARAMS, |row| row.get(0)) {
//            Ok(mode) => mode,
//            Err(e) => {
//                let wait_time = rng.next_u64() % 100;
//                warn!(logger, "Could not set temp store mode: {}", e);
//                warn!(logger, "Will retry in {}ms", wait_time);
//                let wait_dur = Duration::from_millis(wait_time);
//                thread::sleep(wait_dur);
//                String::from("")
//            },
//        };
//
//        if temp_store_mode != String::from("") {
//            break;
//        }
//    }
//
//    debug!(logger, "Journal mode: {}", temp_store_mode);
//    Ok(())
//}

fn busy_callback(i: i32) -> bool {
    let mut rng = rand::thread_rng();

    if i == MAX_DBOPEN_RETRY {
        return false;
    }

    let base: u64 = 2;
    let wait_time = (rng.next_u64() % 30) * base.pow(i as u32);
    eprintln!(
        "Database busy({}). Will retry operation in {}",
        i, wait_time
    );
    let wait_dur = Duration::from_millis(wait_time);
    thread::sleep(wait_dur);

    true
}

/// Function exported exclusively for the use of the Master module.
/// Updates the positions of all registered nodes according to their
/// respective velocity vector. Update happens every 1 second.
pub fn update_worker_positions(conn: &Connection) -> Result<usize, MeshSimError> {
    conn.execute(UPDATE_WORKER_POSITIONS_QRY, NO_PARAMS)
        .map_err(|_| {
            let err_msg = String::from("Failed to update Worker positions");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: None,
            }
        })
}

/// Function exported exclusively for the use of the Master module.
/// Returns ids of all workers that have reached their destination.
pub fn select_final_positions(conn: &Connection) -> Result<Vec<(i64, f64, f64)>, MeshSimError> {
    let mut stmt = conn.prepare(SELECT_REMAINING_DIST_QRY).map_err(|e| {
        let err_msg = String::from("Failed to prepare query SELECT_REMAINING_DIST_QRY");
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    let rows = stmt
        .query_map(NO_PARAMS, |row| {
            let w_id: i64 = row.get(0);
            let target_x: f64 = row.get(1);
            let current_x: f64 = row.get(2);
            let target_y: f64 = row.get(3);
            let current_y: f64 = row.get(4);

            if (target_x - current_x) <= 0.0 && (target_y - current_y) <= 0.0 {
                Some((w_id, current_x, current_y))
            } else {
                None
            }
        })
        .map_err(|e| {
            let err_msg = String::from("Failed to get workers that have reached their destination");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    let mut arrived: Vec<(i64, f64, f64)> = Vec::new();
    for r in rows {
        let r = r.map_err(|e| {
            let err_msg = String::from("Failed to read row");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        if let Some((w_id, x, y)) = r {
            arrived.push((w_id, x, y));
        }
    }

    Ok(arrived)
}

/// Function exported exclusively for the use of the Master module.
/// Sets the velocity of the worker ids to zero.
pub fn stop_workers(
    conn: &Connection,
    w_ids: &[(i64, f64, f64)],
    _logger: &Logger,
) -> Result<usize, MeshSimError> {
    let mut query_builder = String::from(STOP_WORKERS_QRY);
    query_builder.push('(');
    for (id, _x, _y) in w_ids.iter() {
        query_builder.push_str(&format!("{}, ", id));
    }
    let _c = query_builder.pop(); //remove the new line char
    let _c = query_builder.pop(); //remove last comma
                                  //Close query
    query_builder.push_str(");");

    // eprintln!("Prepared stmt: {}", &query_builder);
    conn.execute(&query_builder, NO_PARAMS).map_err(|e| {
        let err_msg = String::from("Failed to stop workers");
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
            cause: Some(Box::new(e)),
        }
    })
}

///// Gets the current position and database id of a given worker
//pub fn get_worker_position(conn : &Connection,
//                           worker_id : &str,
//                           logger : &Logger) -> Result<(Position, i64), MeshSimError> {
//    let mut rng = rand::thread_rng();
//
//    //Get the current position for the worker_id received
//    let mut stmt = conn.prepare(GET_WORKER_POS_QRY)?;
//    for _i in 0..MAX_DBOPEN_RETRY {
//        let (wp, id) = match stmt.query_row(&[&worker_id], |row| {
//            let db_id : i64 = row.get(0);
//            let x : f64 = row.get(1);
//            let y : f64 = row.get(2);
//            let pos = Position{ x, y };
//            (pos, db_id)
//        }) {
//            Ok((pos, id)) => {
//                (Some(pos), Some(id))
//            },
//            Err(e) => {
//                warn!(logger, "Failed to get worker position: {}", e);
//                let wait_time = rng.next_u64() % 100;
//                let wait_dur = Duration::from_millis(wait_time);
//                thread::sleep(wait_dur);
//                (None, None)
//            },
//        };
//
//        if wp.is_some() && id.is_some() {
//            let worker_pos = wp.unwrap();
//            let db_id = id.unwrap();
//            return Ok((worker_pos, db_id))
//        }
//    }
//
//    error!(logger, "Could not get position for current worker");
//    Err(WorkerError::Sync(String::from("Could not get position for current worker")))
//
//}

/// Gets the current position and database id of a given worker
pub fn get_worker_position(
    conn: &Connection,
    worker_id: &str,
    _logger: &Logger,
) -> Result<(Position, i64), MeshSimError> {
    //Get the current position for the worker_id received
    conn.prepare(GET_WORKER_POS_QRY)
        .and_then(|mut stmt| {
            stmt.query_row(&[&worker_id], |row| {
                let db_id: i64 = row.get(0);
                let x: f64 = row.get(1);
                let y: f64 = row.get(2);
                let pos = Position { x, y };
                (pos, db_id)
            })
        })
        .map_err(|e| {
            let err_msg = String::from("Failed to get position for worker ") + worker_id;
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: Some(Box::new(e)),
            }
        })
}

///// Returns all workers within RANGE meters of the current position of WORKER_ID
//pub fn get_workers_in_range(conn : &Connection,
//                                worker_id : &str,
//                                range : f64,
//                                logger : &Logger) -> Result<Vec<Peer>, MeshSimError> {
//    let mut rng = rand::thread_rng();
//    let (worker_pos, _db_id) = get_worker_position(&conn, worker_id, &logger)?;
//
//    //Get all other workers and check if they are in range
//    let mut stmt = conn.prepare(SELECT_OTHER_WORKERS_POS_QRY)?;
//    for _i in 0..MAX_DBOPEN_RETRY {
//        match stmt.query_map(&[&worker_id], |row| {
//            let name : String = row.get(0);
//            let id : String = row.get(1);
//            let sr : Option<String> = row.get(2);
//            let lr : Option<String> = row.get(3);
//            let x : f64 = row.get(4);
//            let y : f64 = row.get(5);
//            let d = euclidean_distance(worker_pos.x, worker_pos.y, x, y);
//            //debug!("Worker {} in range {}", &id, d);
//            if d <= range {
//                let mut p = Peer::new();
//                p.id = id;
//                p.name = name;
//                p.short_address = sr;
//                p.long_address = lr;
//                Some(p)
//            } else {
//                None
//            }
//        }) {
//            Ok(workers_iter) => {
//                let mut data = Vec::new();
//                for p in workers_iter {
//                    let p = p?;
//                    if p.is_some() {
//                        data.push(p.unwrap());
//                    }
//                }
//                return Ok(data);
//            },
//            Err(e) => {
//                warn!(logger, "Failed to get workers in range: {}", e);
//                let wait_time = rng.next_u64() % 100;
//                let wait_dur = Duration::from_millis(wait_time);
//                thread::sleep(wait_dur);
//            },
//        }
//    }
//
//    // let data = workers_iter.filter(|x| x.unwrap().is_some()).map(|x| x.unwrap()).collect();
//    error!(logger, "Could not get nodes in range of this worker");
//    Err(WorkerError::Sync(String::from("Could not get nodes in range of this worker")))
//}

/// Returns all workers within RANGE meters of the current position of WORKER_ID
pub fn get_workers_in_range(
    conn: &Connection,
    worker_id: &str,
    range: f64,
    logger: &Logger,
) -> Result<Vec<Peer>, MeshSimError> {
    let mut peers = Vec::new();
    let (worker_pos, _db_id) = get_worker_position(&conn, worker_id, &logger)?;

    //Get all other workers and check if they are in range
    let mut stmt = conn.prepare(SELECT_OTHER_WORKERS_POS_QRY).map_err(|e| {
        let err_msg = format!(
            "Failed to prepare query SELECT_OTHER_WORKERS_POS_QRY for worker {}",
            worker_id
        );
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    let rows = stmt
        .query_map(&[&worker_id], |row| {
            let name: String = row.get(0);
            let id: String = row.get(1);
            let sr: Option<String> = row.get(2);
            let lr: Option<String> = row.get(3);
            let x: f64 = row.get(4);
            let y: f64 = row.get(5);
            let d = euclidean_distance(worker_pos.x, worker_pos.y, x, y);
            //debug!("Worker {} in range {}", &id, d);
            if d <= range {
                let mut p = Peer::new();
                p.id = id;
                p.name = name;
                p.short_address = sr;
                p.long_address = lr;
                Some(p)
            } else {
                None
            }
        })
        .map_err(|e| {
            let err_msg = String::from("SELECT_OTHER_WORKERS_POS_QRY failed");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    for row in rows {
        let row = row.map_err(|e| {
            let err_msg = String::from("Failed to read row");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

        if let Some(peer) = row {
            peers.push(peer);
        }
    }

    Ok(peers)
}

/// Calculate the euclidean distance between 2 points.
pub fn euclidean_distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    let xs = (x2 - x1).powf(2.0);
    let ys = (y2 - y1).powf(2.0);
    (xs + ys).sqrt()
}

/// Create a new target position for a worker
pub fn update_worker_target(
    conn: &Connection,
    worker_id: i64,
    target_pos: Position,
    logger: &Logger,
) -> Result<(), MeshSimError> {
    let w_id: &dyn ToSql = &worker_id;
    let dest_x: &dyn ToSql = &target_pos.x;
    let dest_y: &dyn ToSql = &target_pos.y;

    conn.execute(UPDATE_WORKER_DEST_QRY, &[w_id, dest_x, dest_y])
        .map(|_| info!(logger, "Worker_id {} destination updated", worker_id))
        .map_err(|e| {
            let err_msg = format!("Failed to update destination for worker {}", worker_id);
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: Some(Box::new(e)),
            }
        })
}

// TODO: Replace return type for a new WorkerSnapshot type
/// Get the positions of all workers
pub fn get_all_worker_positions(
    conn: &Connection,
) -> Result<Vec<(i64, String, f64, f64)>, MeshSimError> {
    let mut stmt = conn.prepare(SELECT_ALL_WORKERS_POS_QRY).map_err(|e| {
        let err_msg = String::from("Failed to prepare query SELECT_ALL_WORKERS_POS_QRY");
        MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
            cause: Some(Box::new(e)),
        }
    })?;

    let rows = stmt
        .query_map(NO_PARAMS, |row| {
            let w_id: i64 = row.get(0);
            let name: String = row.get(1);
            let x: f64 = row.get(2);
            let y: f64 = row.get(3);
            (w_id, name, x, y)
        })
        .map_err(|e| {
            let err_msg = String::from("Failed to get position for all workers");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

    let mut results: Vec<(i64, String, f64, f64)> = Vec::new();
    for row in rows {
        let row = row.map_err(|e| {
            let err_msg = String::from("Failed to read row");
            MeshSimError {
                kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        results.push((row.0, row.1, row.2, row.3));
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    extern crate chrono;
    extern crate rand;
    extern crate rusqlite;
    extern crate slog;

    use super::*;

    use self::chrono::prelude::*;
    use self::rand::Rng;
    use crate::logging;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::Duration;

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

    /*******************************************
     *******************************************
     ********************************************/

    //Unit test for creating db file
    #[test]
    fn test_get_db_connection() {
        //Add random wait to avoid collitions on the same DB file
        let mut rng = rand::thread_rng();
        let wait: u64 = rng.gen::<u64>() % 100;
        std::thread::sleep(Duration::from_millis(wait));
        let work_dir = create_test_dir("mobility");
        let logger = logging::create_discard_logger();

        let _conn = get_db_connection(&work_dir, &logger).expect("Could not create DB file");
        let expected_file = PathBuf::from(&format!(
            "{}{}{}",
            work_dir,
            std::path::MAIN_SEPARATOR,
            DB_NAME
        ));

        assert!(expected_file.exists());

        //Test passed. Results are not needed.
        fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
    }

    //Unit test for creating the tables in the db
    #[test]
    fn test_create_positions_db() {
        //Add random wait to avoid collitions on the same DB file
        let mut rng = rand::thread_rng();
        let wait: u64 = rng.gen::<u64>() % 100;
        std::thread::sleep(Duration::from_millis(wait));

        let QRY_TABLE_EXISTS = "SELECT rowid FROM sqlite_master WHERE type='table' AND name = (?);";
        let work_dir = create_test_dir("mobility");
        let logger = logging::create_discard_logger();

        let conn = get_db_connection(&work_dir, &logger).expect("Could not create DB file");
        let _res = create_db_objects(&conn, &logger).expect("Could not create DB tables");

        let mut stmt = conn
            .prepare(QRY_TABLE_EXISTS)
            .expect("Could not prepare statement");
        let mut row_id: i64 = -1;
        row_id = stmt
            .query_row(&["workers"], |row| row.get(0))
            .expect("Could not execute query");
        assert_ne!(row_id, -1);

        row_id = stmt
            .query_row(&["worker_positions"], |row| row.get(0))
            .expect("Could not execute query");
        assert_ne!(row_id, 0);

        row_id = stmt
            .query_row(&["worker_velocities"], |row| row.get(0))
            .expect("Could not execute query");
        assert_ne!(row_id, 0);

        row_id = stmt
            .query_row(&["worker_destinations"], |row| row.get(0))
            .expect("Could not execute query");
        assert_ne!(row_id, 0);

        //Test passed. Results are not needed.
        fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
    }

    //Unit test for confirming a worker is registered in the db
    #[test]
    fn test_register_worker() {
        //Add random wait to avoid collitions on the same DB file
        let mut rng = rand::thread_rng();
        let wait: u64 = rng.gen::<u64>() % 100;
        std::thread::sleep(Duration::from_millis(wait));

        let work_dir = create_test_dir("mobility");
        let logger = logging::create_discard_logger();

        info!(logger, "Test results placed in {}", &work_dir);

        let conn = get_db_connection(&work_dir, &logger).expect("Could not create DB file");

        let _res = create_db_objects(&conn, &logger).expect("Could not create positions table");
        let name = String::from("worker1");
        let id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary id
        let pos = Position { x: 0.0, y: 0.0 };
        let vel = Velocity { x: 0.0, y: 0.0 };
        let sr = String::from("/tmp/sr.socket");
        let lr = String::from("/tmp/lr.socket");

        let worker_db_id = register_worker(
            &conn,
            name,
            &id,
            &pos,
            &vel,
            &None,
            Some(sr),
            Some(lr),
            &logger,
        )
        .expect("Could not register worker");

        assert!(worker_db_id > 0);

        //Test passed. Results are not needed.
        fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
    }

    //Unit test for checking peers are within range.
    #[test]
    fn test_workers_in_range() {
        //Add random wait to avoid collitions on the same DB file
        let mut rng = rand::thread_rng();
        let wait: u64 = rng.gen::<u64>() % 100;
        std::thread::sleep(Duration::from_millis(wait));

        let work_dir = create_test_dir("mob");
        let logger = logging::create_discard_logger();

        info!(logger, "Test results placed in {}", &work_dir);

        let conn = get_db_connection(&work_dir, &logger).expect("Could not create DB file");
        let _res = create_db_objects(&conn, &logger).expect("Could not create positions table");

        //Create test worker1
        let source_name = String::from("worker1");
        let source_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary id
        let source_pos = Position { x: 0.0, y: 0.0 };
        let vel = Velocity { x: 0.0, y: 0.0 };
        let source_sr = String::from("/tmp/sr_1.socket");
        let source_lr = String::from("/tmp/lr_1.socket");
        let _id = register_worker(
            &conn,
            source_name,
            &source_id,
            &source_pos,
            &vel,
            &None,
            Some(source_sr),
            Some(source_lr),
            &logger,
        )
        .expect("Could not register worker1");

        //Create test worker2
        //This worker should be in range.
        let name = String::from("worker2");
        let id = String::from("416d77337e24399dc7a5aa058039f72b");
        let pos = Position { x: 50.0, y: -25.50 };
        let sr = String::from("/tmp/sr_2.socket");
        let lr = String::from("/tmp/lr_2.socket");
        let _id = register_worker(
            &conn,
            name,
            &id,
            &pos,
            &vel,
            &None,
            Some(sr),
            Some(lr),
            &logger,
        )
        .expect("Could not register worker2");

        //Create test worker3
        //This worker should be in range.
        let name = String::from("worker3");
        let id = String::from("416d77337e24399dc7a5aa058039f72c");
        let pos = Position { x: 30.0, y: 45.0 };
        let sr = String::from("/tmp/sr_3.socket");
        let lr = String::from("/tmp/lr_3.socket");
        let _id = register_worker(
            &conn,
            name,
            &id,
            &pos,
            &vel,
            &None,
            Some(sr),
            Some(lr),
            &logger,
        )
        .expect("Could not register worker3");

        //Create test worker4
        //This worker should NOT be in range.
        let name = String::from("worker4");
        let id = String::from("416d77337e24399dc7a5aa058039f72d");
        let pos = Position { x: 120.0, y: 0.0 };
        let sr = String::from("/tmp/sr_4.socket");
        let lr = String::from("/tmp/lr_4.socket");
        let _id = register_worker(
            &conn,
            name,
            &id,
            &pos,
            &vel,
            &None,
            Some(sr),
            Some(lr),
            &logger,
        )
        .expect("Could not register worker4");

        //Create test worker5
        //This worker should NOT be in range.
        let name = String::from("worker5");
        let id = String::from("416d77337e24399dc7a5aa058039f72e");
        let pos = Position { x: 60.0, y: 90.0 };
        let sr = String::from("/tmp/sr_5.socket");
        let lr = String::from("/tmp/lr_5.socket");
        let _id = register_worker(
            &conn,
            name,
            &id,
            &pos,
            &vel,
            &None,
            Some(sr),
            Some(lr),
            &logger,
        )
        .expect("Could not register worker5");

        let workers_in_range: Vec<Peer> =
            get_workers_in_range(&conn, &source_id, 100.0, &logger).unwrap();
        println!("{:?}", &workers_in_range);
        assert_eq!(workers_in_range.len(), 2);

        //Test passed. Results are not needed.
        fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
    }

    //Unit test for checking peers are within range.
    #[test]
    fn test_workers_mobility() {
        use std::collections::HashMap;

        let work_dir = create_test_dir("basic_mobility");
        let logger = logging::create_discard_logger();
        info!(logger, "Test results placed in {}", &work_dir);

        let conn = get_db_connection(&work_dir, &logger).expect("Could not create DB file");
        let _res = create_db_objects(&conn, &logger).expect("Could not create positions table");

        //Create test worker1
        let source_name = String::from("worker1");
        let source_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary id
        let source_pos = Position { x: 0.0, y: 0.0 };
        let vel1 = Velocity { x: 1.0, y: 0.0 };
        let source_sr = String::from("/tmp/sr_1.socket");
        let source_lr = String::from("/tmp/lr_1.socket");
        let _id = register_worker(
            &conn,
            source_name,
            &source_id,
            &source_pos,
            &vel1,
            &None,
            Some(source_sr),
            Some(source_lr),
            &logger,
        )
        .expect("Could not register worker1");

        //Create test worker2
        //This worker should be in range.
        let name = String::from("worker2");
        let id = String::from("416d77337e24399dc7a5aa058039f72b");
        let pos = Position { x: 50.0, y: -25.50 };
        let vel2 = Velocity { x: 1.0, y: -1.5 };
        let sr = String::from("/tmp/sr_2.socket");
        let lr = String::from("/tmp/lr_2.socket");
        let _id = register_worker(
            &conn,
            name,
            &id,
            &pos,
            &vel2,
            &None,
            Some(sr),
            Some(lr),
            &logger,
        )
        .expect("Could not register worker2");

        //Create test worker3
        //This worker should be in range.
        let name = String::from("worker3");
        let id = String::from("416d77337e24399dc7a5aa058039f72c");
        let pos = Position { x: 30.0, y: 45.0 };
        let vel3 = Velocity { x: -3.0, y: 1.5 };
        let sr = String::from("/tmp/sr_3.socket");
        let lr = String::from("/tmp/lr_3.socket");
        let _id = register_worker(
            &conn,
            name,
            &id,
            &pos,
            &vel3,
            &None,
            Some(sr),
            Some(lr),
            &logger,
        )
        .expect("Could not register worker3");

        let rows = update_worker_positions(&conn).expect("Could not update worker positions");
        assert_eq!(rows, 3);

        let SELECT_POSITIONS_QRY = "
        SELECT Workers.Worker_ID,
               Worker_positions.X,
               Worker_positions.Y
        FROM Workers
        JOIN Worker_positions on Workers.ID = Worker_positions.worker_id";
        let mut stmt = conn
            .prepare(SELECT_POSITIONS_QRY)
            .expect("Could not prepare statement");
        let pos_iter = stmt
            .query_map(NO_PARAMS, |row| {
                let wid = row.get(0);
                let x = row.get(1);
                let y = row.get(2);
                (wid, x, y)
            })
            .expect("Could not execute query");

        let mut positions = HashMap::new();
        for p in pos_iter {
            let (w, x, y): (String, f64, f64) = p.unwrap();
            positions.insert(w, (x, y));
        }

        //Worker1
        let w1 = positions.get("416d77337e24399dc7a5aa058039f72a").unwrap();
        assert_eq!(w1.0, 1.0);
        assert_eq!(w1.1, 0f64);

        //Worker2
        let w2 = positions.get("416d77337e24399dc7a5aa058039f72b").unwrap();
        assert_eq!(w2.0, 51.0);
        assert_eq!(w2.1, -27.0);

        //Worker3
        let w3 = positions.get("416d77337e24399dc7a5aa058039f72c").unwrap();
        assert_eq!(w3.0, 27.0);
        assert_eq!(w3.1, 46.5);

        //Test passed. Results are not needed.
        fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
    }

    #[test]
    fn test_stop_workers() {
        let mut rng = rand::thread_rng();
        let num: u64 = rng.gen::<u64>() % 10000;

        let work_dir = create_test_dir(&format!("stop_workers{}", num));
        let logger = logging::create_discard_logger();

        info!(logger, "Test results placed in {}", &work_dir);

        let conn = get_db_connection(&work_dir, &logger).expect("Could not create DB file");
        let _res = create_db_objects(&conn, &logger).expect("Could not create positions table");

        //Create test worker1
        let source_name = String::from("worker1");
        let source_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary id
        let x1 = 0.0;
        let y1 = 0.0;
        let source_pos = Position { x: 0.0, y: 0.0 };
        let vel = Velocity { x: 1.0, y: 0.7 };
        let source_sr = String::from("/tmp/sr_1.socket");
        let source_lr = String::from("/tmp/lr_1.socket");
        let id1 = register_worker(
            &conn,
            source_name,
            &source_id,
            &source_pos,
            &vel,
            &None,
            Some(source_sr),
            Some(source_lr),
            &logger,
        )
        .expect("Could not register worker1");

        //Create test worker2
        //This worker should be in range.
        let name = String::from("worker2");
        let id = String::from("416d77337e24399dc7a5aa058039f72b");
        let x2 = 50.0;
        let y2 = -25.50;
        let pos = Position { x: 50.0, y: -25.50 };
        let vel = Velocity { x: -1.0, y: 2.1 };
        let sr = String::from("/tmp/sr_2.socket");
        let lr = String::from("/tmp/lr_2.socket");
        let id2 = register_worker(
            &conn,
            name,
            &id,
            &pos,
            &vel,
            &None,
            Some(sr),
            Some(lr),
            &logger,
        )
        .expect("Could not register worker2");

        let rows = stop_workers(&conn, &[(id1, x1, y1), (id2, x2, y2)], &logger)
            .expect("Could not stop workers");

        assert_eq!(rows, 2);

        //Test passed. Results are not needed.
        fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");
    }

    #[test]
    fn test_dist_func() {
        //Add random wait to avoid collitions on the same DB file
        let mut rng = rand::thread_rng();
        let wait: u64 = rng.gen::<u64>() % 100;
        std::thread::sleep(Duration::from_millis(wait));
        let work_dir = create_test_dir("distance_func");
        let logger = logging::create_discard_logger();

        let conn = get_db_connection(&work_dir, &logger).expect("Could not create DB file");
        let expected_distance: f64 = 141.4213562373095;
        let obtained_distance: f64 = conn
            .query_row("SELECT distance(0, 0, 100, 100)", NO_PARAMS, |r| r.get(0))
            .expect("Could not exec query");

        assert_eq!(expected_distance, obtained_distance);

        //Test passed. Results are not needed.
        fs::remove_dir_all(&work_dir).expect("Failed to remove results directory");

        //        //Add random wait to avoid collitions on the same DB file
        //        let mut rng = rand::thread_rng();
        //        let wait : u64 = rng.gen::<u64>() % 100;
        //        std::thread::sleep(Duration::from_millis(wait));
        //
        //        let path = create_test_dir("func");
        //        let logger = logging::create_discard_logger();
        //
        //        info!(logger, "Test results placed in {}", &path);
        //
        //        let conn = get_db_connection(&path, &logger).expect("Could not create DB file");
    }
}
