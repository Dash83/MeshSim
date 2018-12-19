//! This module handles the position and mobility features of the worker
//! 
extern crate rusqlite;

use self::rusqlite::types::ToSql;
use self::rusqlite::{Connection, NO_PARAMS, OpenFlags};
use std::path::PathBuf;
use worker::WorkerError;

const DB_NAME : &'static str = "worker_positions.db";
const CREATE_WORKERS_TBL_QRY : &'static str = "CREATE TABLE IF NOT EXISTS Workers (
                                                    ID	                INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                                                    Worker_Name	        TEXT NOT NULL,
                                                    Worker_ID	        TEXT NOT NULL UNIQUE,
                                                    X	                REAL NOT NULL,
                                                    Y                   REAL NOT NULL
                                            )";
const INSERT_WORKER_QRY : &'static str = "INSERT INTO Workers (Worker_Name, Worker_ID, X, Y)
                                          VALUES (?1, ?2, ?3, ?4)";
const SELECT_WORKER_QRY : &'static str = "SELECT ID, Worker_Name, Worker_ID, X, Y FROM Workers WHERE Worker_ID = (?)";
                                          
///Struct to encapsule the 2D position of the worker
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Position {
    /// X component
    pub x : f64,
    /// Y component
    pub y : f64,
}

/// Get a connection to the current DB
pub fn get_db_connection<'a>(path : &'a str ) -> Result<Connection, WorkerError> {
    let mut dir = PathBuf::from(path);
    dir.push(DB_NAME);
    let c = rusqlite::Connection::open_with_flags(dir, OpenFlags::SQLITE_OPEN_SHARED_CACHE |
                                                        OpenFlags::SQLITE_OPEN_READ_WRITE | 
                                                        OpenFlags::SQLITE_OPEN_CREATE)?;
    Ok(c)
}

/// Crate the database tables
pub fn create_positions_db(conn : &Connection) -> Result<usize, WorkerError> {
    let rows = conn.execute(CREATE_WORKERS_TBL_QRY, NO_PARAMS,)?;
    Ok(rows)
}

/// Registers a newly created worker into the mobility system
pub fn register_worker(conn : &Connection, worker_name : String, worker_id : String, pos : &Position) -> Result<i64, WorkerError> {
    let name : &ToSql = &worker_name;
    let wid : &ToSql = &worker_id;
    let x : &ToSql = &pos.x;
    let y : &ToSql = &pos.y;
    let rows = conn.execute( INSERT_WORKER_QRY, &[name, wid, x, y] )?;

    if rows <= 0 {
        return Err(WorkerError::Sync(format!("Could not register worker {} in the DB", &worker_id)));
    }

    let mut stmt = conn.prepare(SELECT_WORKER_QRY)?;
    let db_id : i64 = stmt.query_row(&[&worker_id], |row| row.get(0))?;

    Ok(db_id)
}

    //    //Create the DB if not there and register this worker
    //     let mut dir = std::fs::canonicalize(&self.work_dir)?;
    //     dir.push(DB_NAME);
    //     let db_conn = SimulatedRadio::get_db_connection(&dir)?;
    //     let rows = SimulatedRadio::create_positions_db(&db_conn)?;

    //     if rows > 0 {
    //         info!("Positions DB creaded");
    //     }