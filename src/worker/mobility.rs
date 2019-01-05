//! This module handles the position and mobility features of the worker
//! 
extern crate rusqlite;
extern crate rand;

use self::rusqlite::types::ToSql;
use self::rusqlite::{Connection, NO_PARAMS, OpenFlags};
use std::path::PathBuf;
use worker::{WorkerError, Peer};
// use self::rand::prelude::*;
use self::rand::{Rng, StdRng, RngCore};
use std::time::Duration;
use std::thread;

/// Name used for the worker positions DB
pub const DB_NAME : &'static str = "worker_positions.db";
const MAX_DBOPEN_RETRY : i32 = 4;
const CREATE_WORKERS_TBL_QRY : &'static str = "CREATE TABLE IF NOT EXISTS Workers (
                                                    ID	                    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                                                    Worker_Name	            TEXT NOT NULL,
                                                    Worker_ID	            TEXT NOT NULL UNIQUE,
                                                    X	                    REAL NOT NULL,
                                                    Y                       REAL NOT NULL,
                                                    Short_Range_Address	    TEXT,
                                                    Long_Range_Address	    TEXT
                                            )";
const INSERT_WORKER_QRY : &'static str = "INSERT INTO Workers (Worker_Name, Worker_ID, X, Y, Short_Range_Address, Long_Range_Address)
                                          VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
const SELECT_WORKER_QRY : &'static str = "SELECT ID, Worker_Name, Worker_ID, X, Y, Short_Range_Address, Long_Range_Address FROM Workers WHERE Worker_ID = (?)";
const SELECT_OTHER_WORKERS_QRY : &'static str = "SELECT Worker_Name, Worker_ID, X, Y, Short_Range_Address, Long_Range_Address FROM Workers WHERE Worker_ID != (?)";
const SET_WAL_MODE : &'static str = "PRAGMA journal_mode=WAL;";
const WAL_MODE_QRY : &'static str = "PRAGMA journal_mode;";

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
    let mut rng = rand::thread_rng();
    let mut conn : Option<Connection> = None;

    let mut dir = PathBuf::from(path);
    dir.push(DB_NAME);

    for _i in 0..MAX_DBOPEN_RETRY {
        match rusqlite::Connection::open_with_flags(&dir, OpenFlags::SQLITE_OPEN_SHARED_CACHE |
                                                        OpenFlags::SQLITE_OPEN_READ_WRITE | 
                                                        OpenFlags::SQLITE_OPEN_CREATE) {
            Ok(c) => { 
                conn = Some(c);
            },
            Err(e) => { 
                warn!("Failed to establish DB connection: {}", e);
                let wait_time = rng.next_u64() % 100;
                let wait_dur = Duration::from_millis(wait_time);
                thread::sleep(wait_dur);
            },
        }

        if let Some(c) = conn {
            let _res = c.busy_handler(Some(busy_callback));
            return Ok(c)
        }
    }
    error!("Could not establish a DB connection");
    Err(WorkerError::Sync(String::from("Could not establish a DB connection")))
}

/// Crate the database tables
pub fn create_positions_db(conn : &Connection) -> Result<usize, WorkerError> {
    //Set the DB journaling mode to WAL first.
    let _ = set_wal_mode(conn)?;
    let rows = conn.execute(CREATE_WORKERS_TBL_QRY, NO_PARAMS,)?;
    Ok(rows)
}

/// Registers a newly created worker into the mobility system
pub fn register_worker( conn : &Connection, 
                        worker_name : String, 
                        worker_id : &String, 
                        pos : &Position, 
                        sr_address : Option<String>, 
                        lr_address : Option<String>) -> Result<i64, WorkerError> {
    let name : &ToSql = &worker_name;
    let wid : &ToSql = &worker_id;
    let x : &ToSql = &pos.x;
    let y : &ToSql = &pos.y;
    let sr : &ToSql = &sr_address;
    let lr : &ToSql = &lr_address;
    let mut rows = 0;
    let mut rng = rand::thread_rng();

    for i in 0..MAX_DBOPEN_RETRY {
        rows = match conn.execute( INSERT_WORKER_QRY, &[name, wid, x, y, sr, lr] ) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to register worker: {}", e);
                let wait_time = rng.next_u64() % 100;
                info!("Will retry in {} ms", wait_time);
                let wait_dur = Duration::from_millis(wait_time);
                thread::sleep(wait_dur);
                0
            }
        };

        if rows > 0 {
            //Worker registered
            info!("Worker registered in DB successfully");
            break;
        } else if i+1 == MAX_DBOPEN_RETRY {
            return Err(WorkerError::Sync(format!("Could not register worker {} in the DB", &worker_id)));
        }
    }

    let mut stmt = conn.prepare(SELECT_WORKER_QRY)?;
    debug!("Select query prepapred");
    let db_id : i64 = stmt.query_row(&[&worker_id], |row| row.get(0))?;

    Ok(db_id)
}

fn set_wal_mode(conn : &Connection) -> Result<(), WorkerError> {
    let mut rng = rand::thread_rng();
    let mut stmt = conn.prepare(WAL_MODE_QRY)?;
    let mut journal_mode = String::from("");
    
    for i in 0..MAX_DBOPEN_RETRY {
        journal_mode = match stmt.query_row(NO_PARAMS, |row| row.get(0)) {
            Ok(mode) => mode,
            Err(e) => {
                let wait_time = rng.next_u64() % 100;
                warn!("Could not read journal mode. Will retry in {}ms", wait_time);
                let wait_dur = Duration::from_millis(wait_time);
                thread::sleep(wait_dur);
                String::from("")
            },
        };

        if journal_mode != String::from("") {
            break;
        }
    }

    if journal_mode.to_uppercase() == String::from("WAL") {
        debug!("WAL mode already set");
        return Ok(())
    }

    stmt = conn.prepare(SET_WAL_MODE)?;
    for _i in 0..MAX_DBOPEN_RETRY {
        journal_mode = match stmt.query_row(NO_PARAMS, |row| row.get(0)) {
            Ok(mode) => mode,
            Err(e) => {
                let wait_time = rng.next_u64() % 100;
                warn!("Could not read journal mode: {}", e); 
                warn!("Will retry in {}ms", wait_time);
                let wait_dur = Duration::from_millis(wait_time);
                thread::sleep(wait_dur);
                String::from("")
            },
        };

        if journal_mode != String::from("") {
            break;
        }
    }

    debug!("Journal mode: {}", journal_mode);
    Ok(())
}

fn busy_callback(i : i32) -> bool {
    let mut rng = rand::thread_rng();

    if i == MAX_DBOPEN_RETRY {
        return false
    }

    let wait_time = rng.next_u64() % 100;
    warn!("Database busy. Will retry operation in {}", wait_time);
    let wait_dur = Duration::from_millis(wait_time);
    thread::sleep(wait_dur);
    
    true
}

/// Returns all workers within RANGE meters of the current position of WORKER_ID
pub fn get_workers_in_range<'a>(conn : &Connection, worker_id : &'a str, range : f64) -> Result<Vec<Peer>, WorkerError> {
    //Get the current position for the worker_id received
    let mut stmt = conn.prepare(SELECT_WORKER_QRY)?;
    let worker_pos : Position = stmt.query_row(&[&worker_id], |row| { 
        let x : f64 = row.get(3);
        let y : f64 = row.get(4);
        Position{ x : x, 
                  y : y,
                }
    })?;

    //Get all other workers and check if they are in range
    let mut stmt = conn.prepare(SELECT_OTHER_WORKERS_QRY)?;
    let workers_iter = stmt.query_map(&[&worker_id], |row| {
        let id : String = row.get(0);
        let name : String = row.get(1);
        let x : f64 = row.get(2);
        let y : f64 = row.get(3);
        let sr : Option<String> = row.get(4);
        let lr : Option<String> = row.get(5);
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
    })?;
        
    // let data = workers_iter.filter(|x| x.unwrap().is_some()).map(|x| x.unwrap()).collect();
    let mut data = Vec::new();
    for p in workers_iter {
        let p = p?;
        if p.is_some() {
            data.push(p.unwrap());
        }
    }
    Ok(data)
}

fn euclidean_distance(x1 : f64, y1 : f64, x2 : f64, y2 : f64) -> f64 {
    let xs = (x2 - x1).powf(2.0);
    let ys = (y2 - y1).powf(2.0);
    let d = (xs + ys).sqrt();
    d
}

#[cfg(test)]
mod tests {
    extern crate chrono;
    extern crate rand;

    use super::*;
    use self::chrono::prelude::*;
    use std::path::{PathBuf, Path};
    use std::fs;
    use self::rand::Rng;
    use std::time::Duration;

/******************************************* 
 *********** Utility functions *************
********************************************/
    fn get_tests_root() -> String {
        use std::env;
        let test_home = env::var("MESHSIM_TEST_DIR").unwrap_or(String::from("/tmp/"));
        test_home
    }

    fn create_test_dir<'a>(test_name : &'a str) -> String {
        let now : DateTime<Utc> = Utc::now();
        let test_dir_path = format!("{}{}_{}", &get_tests_root(), 
                                                test_name,
                                                now.timestamp());
        let test_dir = Path::new(&test_dir_path);

        if !test_dir.exists() {
            fs::create_dir(&test_dir_path).expect(&format!("Unable to create test results directory {}", test_dir_path));
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
        let wait : u64 = rng.gen::<u64>() % 100;
        std::thread::sleep(Duration::from_millis(wait));
        let path = create_test_dir("mobility");
        let _conn = get_db_connection(&path).expect("Could not create DB file");
        let expected_file = PathBuf::from(&format!("{}{}{}", path, std::path::MAIN_SEPARATOR, DB_NAME));

        assert!(expected_file.exists());
    }

    //Unit test for creating the tables in the db
    #[test]
    fn test_create_positions_db() {
        //Add random wait to avoid collitions on the same DB file
        let mut rng = rand::thread_rng();
        let wait : u64 = rng.gen::<u64>() % 100;
        std::thread::sleep(Duration::from_millis(wait));
        
        let QRY_TABLE_EXISTS = "SELECT rowid FROM sqlite_master WHERE type='table' AND name = (?);";
        let path = create_test_dir("mobility");
        let conn = get_db_connection(&path).expect("Could not create DB file");
        let _res = create_positions_db(&conn).expect("Could not create positions table");

        let mut stmt = conn.prepare(QRY_TABLE_EXISTS).expect("Could not prepare statement");
        let mut row_id : i64 = -1;
        row_id = stmt.query_row(&["Workers"], |row| row.get(0)).expect("Could not execute query");

        assert_ne!(row_id, -1);
    }

    //Unit test for confirming a worker is registered in the db
    #[test]
    fn test_register_worker() {
        //Add random wait to avoid collitions on the same DB file
        let mut rng = rand::thread_rng();
        let wait : u64 = rng.gen::<u64>() % 100;
        std::thread::sleep(Duration::from_millis(wait));
        
        let path = create_test_dir("mobility");
        println!("Test results placed in {}", &path);

        let conn = get_db_connection(&path).expect("Could not create DB file");

        let _res = create_positions_db(&conn).expect("Could not create positions table");
        let name = String::from("worker1");
        let id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary id
        let pos = Position{ x : 0.0, y : 0.0 };
        let sr = String::from("/tmp/sr.socket");
        let lr = String::from("/tmp/lr.socket");

        let worker_db_id = register_worker(&conn, name, &id, &pos, Some(sr), Some(lr)).expect("Could not register worker");
        
        assert!(worker_db_id > 0);
    }

    //Unit test for checking peers are within range.
    #[test]
    fn test_workers_in_range() {
        //Add random wait to avoid collitions on the same DB file
        let mut rng = rand::thread_rng();
        let wait : u64 = rng.gen::<u64>() % 100;
        std::thread::sleep(Duration::from_millis(wait));
        
        let path = create_test_dir("mob");
        println!("Test results placed in {}", &path);
        let conn = get_db_connection(&path).expect("Could not create DB file");
        let _res = create_positions_db(&conn).expect("Could not create positions table");

        //Create test worker1
        let source_name = String::from("worker1");
        let source_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary id
        let source_pos = Position{ x : 0.0, y : 0.0 };
        let source_sr = String::from("/tmp/sr_1.socket");
        let source_lr = String::from("/tmp/lr_1.socket");
        let _id = register_worker(&conn, source_name, &source_id, &source_pos, Some(source_sr), Some(source_lr)).expect("Could not register worker1");

        //Create test worker2
        //This worker should be in range.
        let name = String::from("worker2");
        let id = String::from("416d77337e24399dc7a5aa058039f72b");
        let pos = Position{ x : 50.0, y : -25.50 };
        let sr = String::from("/tmp/sr_2.socket");
        let lr = String::from("/tmp/lr_2.socket");
        let _id = register_worker(&conn, name, &id, &pos, Some(sr), Some(lr)).expect("Could not register worker2");

        //Create test worker3
        //This worker should be in range.
        let name = String::from("worker3");
        let id = String::from("416d77337e24399dc7a5aa058039f72c");
        let pos = Position{ x : 30.0, y : 45.0 };
        let sr = String::from("/tmp/sr_3.socket");
        let lr = String::from("/tmp/lr_3.socket");
        let _id = register_worker(&conn, name, &id, &pos, Some(sr), Some(lr)).expect("Could not register worker3");

        //Create test worker4
        //This worker should NOT be in range.
        let name = String::from("worker4");
        let id = String::from("416d77337e24399dc7a5aa058039f72d");
        let pos = Position{ x : 120.0, y : 0.0 };
        let sr = String::from("/tmp/sr_4.socket");
        let lr = String::from("/tmp/lr_4.socket");
        let _id = register_worker(&conn, name, &id, &pos, Some(sr), Some(lr)).expect("Could not register worker4");

        //Create test worker5
        //This worker should NOT be in range.
        let name = String::from("worker5");
        let id = String::from("416d77337e24399dc7a5aa058039f72e");
        let pos = Position{ x : 60.0, y : 90.0 };
        let sr = String::from("/tmp/sr_5.socket");
        let lr = String::from("/tmp/lr_5.socket");
        let _id = register_worker(&conn, name, &id, &pos, Some(sr), Some(lr)).expect("Could not register worker5");

        let workers_in_range : Vec<Peer> = get_workers_in_range(&conn, &source_id, 100.0).unwrap();
        println!("{:?}", &workers_in_range);
        assert_eq!(workers_in_range.len(), 2);
    }
}