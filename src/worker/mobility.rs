//! This module handles the position and mobility features of the worker
//! 
extern crate rusqlite;
extern crate rand;

use self::rusqlite::types::ToSql;
use self::rusqlite::{Connection, NO_PARAMS, OpenFlags};
use std::path::PathBuf;
use worker::{WorkerError, Peer};
use self::rand::RngCore;
use std::time::Duration;
use std::thread;
use ::slog::Logger;

/// Name used for the worker positions DB
pub const DB_NAME : &'static str = "worker_positions.db";
/// The mean of human walking speeds
pub const HUMAN_SPEED_MEAN : f64 = 1.462; //meters per second.
/// Standard deviation of human walking speeds
pub const HUMAN_SPEED_STD_DEV : f64 = 0.164;
const MAX_DBOPEN_RETRY : i32 = 4;

//region Queries
const CREATE_WORKERS_TBL_QRY : &'static str = "CREATE TABLE IF NOT EXISTS workers (
                                                    ID	                    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                                                    Worker_Name	            TEXT NOT NULL,
                                                    Worker_ID	            TEXT NOT NULL UNIQUE,
                                                    Short_Range_Address	    TEXT,
                                                    Long_Range_Address	    TEXT
                                            )";
const CREATE_WORKER_POS_TBL_QRY : &'static str = "CREATE TABLE IF NOT EXISTS  `worker_positions` (
                                                    `worker_id`	INTEGER UNIQUE,
                                                    X	REAL NOT NULL,
                                                    Y  REAL NOT NULL,
                                                    FOREIGN KEY(worker_id) REFERENCES workers(ID)
                                                );";

const CREATE_WORKER_VEL_TBL_QRY : &'static str = "CREATE TABLE IF NOT EXISTS  `worker_velocities` (
                                                    `worker_id`	INTEGER UNIQUE NOT NULL,
                                                    vel_x	REAL NOT NULL,
                                                    vel_y  REAL NOT NULL,
                                                    FOREIGN KEY(worker_id) REFERENCES workers(ID)
                                                );";
const CREATE_WORKER_DEST_TBL_QRY : &'static str = "CREATE TABLE IF NOT EXISTS  `worker_destinations` (
                                                    `worker_id`	INTEGER UNIQUE NOT NULL,
                                                    `dest_x`	INTEGER NOT NULL,
                                                    `dest_y`	INTEGER NOT NULL,
                                                    FOREIGN KEY(`worker_id`) REFERENCES `workers`(`ID`)
                                                );";

const INSERT_WORKER_QRY : &'static str = "INSERT INTO workers (Worker_Name, Worker_ID, Short_Range_Address, Long_Range_Address)
                                          VALUES (?1, ?2, ?3, ?4)";
const UPDATE_WORKER_POS_QRY : &'static str = "INSERT OR REPLACE INTO worker_positions (worker_id, x, y) VALUES (?1, ?2, ?3)";
const UPDATE_WORKER_VEL_QRY : &'static str = "INSERT OR REPLACE INTO worker_velocities (worker_id, vel_x, vel_y) VALUES (?1, ?2, ?3)";
const UPDATE_WORKER_DEST_QRY : &'static str = "INSERT OR REPLACE INTO worker_destinations (worker_id, dest_x, dest_y) VALUES (?1, ?2, ?3)";
const GET_WORKER_QRY : &'static str = "SELECT ID, Worker_Name, Worker_ID, Short_Range_Address, Long_Range_Address FROM Workers WHERE Worker_ID = (?)";
const SELECT_OTHER_WORKERS_QRY : &'static str = "SELECT Worker_Name, Worker_ID, Short_Range_Address, Long_Range_Address FROM Workers WHERE Worker_ID != (?)";
const GET_WORKER_POS_QRY : &'static str =  "SELECT  Workers.ID,
                                                    Worker_positions.X,
                                                    Worker_positions.Y
                                            FROM Workers
                                            JOIN Worker_positions on Workers.ID = Worker_positions.worker_id
                                            WHERE Workers.Worker_ID = (?)";
const SELECT_OTHER_WORKERS_POS_QRY : &'static str = "SELECT Workers.worker_name,
		                                                    Workers.Worker_ID,
                                                            Workers.short_range_address, 
                                                            Workers.long_range_address, 
                                                            Worker_positions.X,
                                                            Worker_positions.Y
                                                    FROM Workers
                                                    JOIN Worker_positions on Workers.ID = Worker_positions.worker_id
                                                    WHERE Workers.Worker_ID != (?)";
const SELECT_ALL_WORKERS_POS_QRY : &'static str = "SELECT   Workers.ID,
                                                            Workers.worker_name,
                                                            Worker_positions.X,
                                                            Worker_positions.Y
                                                    FROM Workers
                                                    JOIN Worker_positions on Workers.ID = Worker_positions.worker_id";
const SELECT_WORKERS_VEL_QRY : &'static str = "SELECT ID, vel_x, vel_y FROM worker_velocities";
const UPDATE_WORKER_POSITIONS_QRY : &'static str = "
                                            INSERT OR REPLACE INTO worker_positions
                                            (worker_id, x, y)
                                            select worker_positions.worker_id,
                                                    worker_positions.X + worker_velocities.vel_x,
                                                    worker_positions.Y + worker_velocities.vel_y
                                            from worker_velocities
                                            join worker_positions on worker_positions.worker_id = worker_velocities.worker_id;";
const STOP_WORKERS_QRY : &'static str = "
                                UPDATE worker_velocities
                                SET  vel_x = 0,
                                        vel_y = 0
                                WHERE worker_velocities.worker_id in (?);";
const SELECT_REMAINING_DIST_QRY : &'static str = "
                                select workers.ID,
                                        worker_destinations.dest_x,
                                        worker_positions.X,
                                        worker_destinations.dest_y,
                                        worker_positions.Y
                                from workers
                                join worker_positions on worker_positions.worker_id = workers.ID
                                join worker_destinations on worker_destinations.worker_id = workers.ID
                                order by workers.Worker_Name;";
const SET_WAL_MODE : &'static str = "PRAGMA journal_mode=WAL;";
const WAL_MODE_QRY : &'static str = "PRAGMA journal_mode;";
//endregion Queries

///Struct to encapsule the 2D position of the worker
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Position {
    /// X component
    pub x : f64,
    /// Y component
    pub y : f64,
}

///Struct to encapsule the velocity vector of a worker
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct Velocity {
    /// X component
    pub x : f64,
    /// Y component
    pub y : f64,
}

/// Get a connection to the current DB
pub fn get_db_connection<'a>(path : &'a str, logger : &Logger ) -> Result<Connection, WorkerError> {
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
                warn!(logger, "Failed to establish DB connection: {}", e);
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
    error!(logger, "Could not establish a DB connection");
    Err(WorkerError::Sync(String::from("Could not establish a DB connection")))
}

/// Crate the database tables
pub fn create_db_objects(conn : &Connection, logger : &Logger) -> Result<usize, WorkerError> {
    //Set the DB journaling mode to WAL first.
    let _ = set_wal_mode(conn, logger)?;

    //Create workers table
    let rows = conn.execute(CREATE_WORKERS_TBL_QRY, NO_PARAMS,)?;

    //Create positions table
    let rows = conn.execute(CREATE_WORKER_POS_TBL_QRY, NO_PARAMS,)?;

    //Create velocities table
    let rows = conn.execute(CREATE_WORKER_VEL_TBL_QRY, NO_PARAMS,)?;
    
    //Create destinations table
    let rows = conn.execute(CREATE_WORKER_DEST_TBL_QRY, NO_PARAMS,)?;

    Ok(rows)
}

/// Registers a newly created worker into the mobility system
pub fn register_worker( conn : &Connection, 
                        worker_name : String, 
                        worker_id : &String, 
                        pos : &Position, 
                        vel : &Velocity,
                        dest : &Option<Position>,
                        sr_address : Option<String>, 
                        lr_address : Option<String>,
                        logger : &Logger ) -> Result<i64, WorkerError> {
    let name : &ToSql = &worker_name;
    let x : &ToSql = &pos.x;
    let y : &ToSql = &pos.y;
    let sr : &ToSql = &sr_address;
    let lr : &ToSql = &lr_address;


    //Insert worker
    let _res = insert_worker(&conn, name, &worker_id, sr, lr, logger)?;

    //Get the ID that was generated for the worker
    let mut stmt = conn.prepare(GET_WORKER_QRY)?;
    let db_id : i64 = stmt.query_row(&[&worker_id], |row| row.get(0))?;
    
    //Insert position
    let _res = update_worker_position(&conn, x, y, db_id, logger)?;

    //Insert velocity
    let _res = update_worker_vel(&conn, vel, db_id, logger)?;
    
    //Insert destination if any
    if let Some(d) = dest {
        let _res = update_worker_target(&conn, db_id, d.clone(), logger)?;
    }

    Ok(db_id)
}

fn insert_worker(conn : &Connection,
                 name : &ToSql,
                 worker_id : &String,
                 sr : &ToSql,
                 lr : &ToSql,
                 logger : &Logger ) -> Result<usize, WorkerError> {
    let mut rows = 0;
    let mut rng = rand::thread_rng();
    let wid : &ToSql = worker_id;

    for i in 0..MAX_DBOPEN_RETRY {
        rows = match conn.execute( INSERT_WORKER_QRY, &[name, wid, sr, lr] ) {
            Ok(r) => r,
            Err(e) => {
                error!(logger, "Failed to register worker: {}", e);
                let wait_time = rng.next_u64() % 100;
                info!(logger, "Will retry in {} ms", wait_time);
                let wait_dur = Duration::from_millis(wait_time);
                thread::sleep(wait_dur);
                0
            }
        };

        if rows > 0 {
            //Worker registered
            info!(logger, "Worker registered in DB successfully");
            break;
        } else if i+1 == MAX_DBOPEN_RETRY {
            return Err(WorkerError::Sync(format!("Could not register worker {} in the DB", worker_id)));
        }
    }

    Ok(rows)
}

fn update_worker_position(conn : &Connection,
                     x : &ToSql,
                     y : &ToSql,
                     worker_id : i64,
                     logger : &Logger ) -> Result<usize, WorkerError> {
    let mut rows = 0;
    let wid : &ToSql = &worker_id;
    let mut rng = rand::thread_rng();

    for i in 0..MAX_DBOPEN_RETRY {
        rows = match conn.execute( UPDATE_WORKER_POS_QRY, &[wid, x, y] ) {
            Ok(r) => r,
            Err(e) => {
                error!(logger, "Failed to update the position of worker: {}", e);
                let wait_time = rng.next_u64() % 100;
                info!(logger, "Will retry in {} ms", wait_time);
                let wait_dur = Duration::from_millis(wait_time);
                thread::sleep(wait_dur);
                0
            }
        };

        if rows > 0 {
            //Worker registered
            //debug!("Position updated!");
            break;
        } else if i+1 == MAX_DBOPEN_RETRY {
            return Err(WorkerError::Sync(format!("Could not update the position of worker {}", worker_id)));
        }
    }

    Ok(rows)
}

/// Updates the worker's velocity
pub fn update_worker_vel(conn : &Connection,
                     vel : &Velocity,
                     worker_id : i64,
                     logger : &Logger ) -> Result<usize, WorkerError> {
    let vel_x : &ToSql = &vel.x;
    let vel_y : &ToSql = &vel.y;
    let mut rows = 0;
    let wid : &ToSql = &worker_id;
    let mut rng = rand::thread_rng();

    for i in 0..MAX_DBOPEN_RETRY {
        rows = match conn.execute( UPDATE_WORKER_VEL_QRY, &[wid, vel_x, vel_y] ) {
            Ok(r) => r,
            Err(e) => {
                error!(logger, "Failed to update the velocity of worker: {}", e);
                let wait_time = rng.next_u64() % 100;
                info!(logger, "Will retry in {} ms", wait_time);
                let wait_dur = Duration::from_millis(wait_time);
                thread::sleep(wait_dur);
                0
    }
        };

        if rows > 0 {
            //Worker registered
            //debug!("Position updated!");
            break;
        } else if i+1 == MAX_DBOPEN_RETRY {
            return Err(WorkerError::Sync(format!("Could not update the velocity of worker {}", worker_id)));
        }
    }

    Ok(rows)
}

fn set_wal_mode(conn : &Connection, logger : &Logger) -> Result<(), WorkerError> {
    let mut rng = rand::thread_rng();
    let mut stmt = conn.prepare(WAL_MODE_QRY)?;
    let mut journal_mode = String::from("");
    
    for i in 0..MAX_DBOPEN_RETRY {
        journal_mode = match stmt.query_row(NO_PARAMS, |row| row.get(0)) {
            Ok(mode) => mode,
            Err(e) => {
                let wait_time = rng.next_u64() % 100;
                warn!(logger, "Could not read journal mode. Will retry in {}ms", wait_time);
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
        debug!(logger, "WAL mode already set");
        return Ok(())
    }

    stmt = conn.prepare(SET_WAL_MODE)?;
    for _i in 0..MAX_DBOPEN_RETRY {
        journal_mode = match stmt.query_row(NO_PARAMS, |row| row.get(0)) {
            Ok(mode) => mode,
            Err(e) => {
                let wait_time = rng.next_u64() % 100;
                warn!(logger, "Could not read journal mode: {}", e); 
                warn!(logger, "Will retry in {}ms", wait_time);
                let wait_dur = Duration::from_millis(wait_time);
                thread::sleep(wait_dur);
                String::from("")
            },
        };

        if journal_mode != String::from("") {
            break;
        }
    }

    debug!(logger, "Journal mode: {}", journal_mode);
    Ok(())
}

fn busy_callback(i : i32) -> bool {
    let mut rng = rand::thread_rng();

    if i == MAX_DBOPEN_RETRY {
        return false
    }

    let wait_time = rng.next_u64() % 100;
    eprintln!("Database busy. Will retry operation in {}", wait_time);
    let wait_dur = Duration::from_millis(wait_time);
    thread::sleep(wait_dur);
    
    true
}

//**********************************************/
//************** Public functions **************/
//**********************************************/
/// Function exported exclusively for the use of the Master module.
/// Updates the positions of all registered nodes according to their
/// respective velocity vector. Update happens every 1 second. 
pub fn update_worker_positions(conn : &Connection) -> Result<usize, WorkerError> {
    let rows = conn.execute(UPDATE_WORKER_POSITIONS_QRY, NO_PARAMS,)?;
    Ok(rows)
}

/// Function exported exclusively for the use of the Master module.
/// Returns ids of all workers that have reached their destination.
pub fn select_final_positions(conn : &Connection) -> Result<Vec<(i64, f64, f64)>, WorkerError> {
    let mut stmt = conn.prepare(SELECT_REMAINING_DIST_QRY)?;
    let results_iter = stmt.query_map(NO_PARAMS, |row| {
        let w_id : i64 = row.get(0);
        let target_x : f64 = row.get(1);
        let current_x : f64 = row.get(2);
        let target_y : f64 = row.get(3);
        let current_y : f64 = row.get(4);

        if (target_x - current_x) <= 0.0 && 
           (target_y - current_y) <= 0.0 {
            Some((w_id, current_x, current_y))
        } else {
            None
        }
    })?;

    let mut arrived : Vec<(i64, f64, f64)> = Vec::new();
    for r in results_iter {
        let r = r?;
        if r.is_some() {
            let (w_id, x, y) = r.unwrap();
            arrived.push((w_id, x, y));
        }
    }

    Ok(arrived)
}

/// Function exported exclusively for the use of the Master module.
/// Sets the velocity of the worker ids to zero.
pub fn stop_workers(conn : &Connection, w_ids : &[(i64, f64, f64)]) -> Result<usize, WorkerError> {
    let ids : Vec<i64> = w_ids.into_iter().map(|(id, _x, _y)| id.clone()).collect();
    let rows = conn.execute(STOP_WORKERS_QRY, ids)?;
    Ok(rows)
}

/// Returns all workers within RANGE meters of the current position of WORKER_ID
pub fn get_workers_in_range<'a>(conn : &Connection, worker_id : &String, range : f64) -> Result<Vec<Peer>, WorkerError> {
    //Get the current position for the worker_id received
    let mut stmt = conn.prepare(GET_WORKER_POS_QRY)?;
    let (worker_pos, db_id) : (Position, i64) = stmt.query_row(&[&worker_id], |row| { 
        let db_id : i64 = row.get(0);
        let x : f64 = row.get(1);
        let y : f64 = row.get(2);
        let pos = Position{ x : x, y : y};
        (pos, db_id)
    })?;

    //Get all other workers and check if they are in range
    let mut stmt = conn.prepare(SELECT_OTHER_WORKERS_POS_QRY)?;
    let workers_iter = stmt.query_map(&[&worker_id], |row| {
        let name : String = row.get(0);
        let id : String = row.get(1);
        let sr : Option<String> = row.get(2);
        let lr : Option<String> = row.get(3);
        let x : f64 = row.get(4);
        let y : f64 = row.get(5);
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

/// Calculate the euclidean distance between 2 points.
pub fn euclidean_distance(x1 : f64, y1 : f64, x2 : f64, y2 : f64) -> f64 {
    let xs = (x2 - x1).powf(2.0);
    let ys = (y2 - y1).powf(2.0);
    let d = (xs + ys).sqrt();
    d
}

/// Create a new target position for a worker
pub fn update_worker_target(conn : &Connection, 
                         worker_id : i64, 
                         target_pos : Position,
                         logger : &Logger) -> Result<(), WorkerError> {
    let w_id : &ToSql = &worker_id;
    let dest_x : &ToSql = &target_pos.x;
    let dest_y : &ToSql = &target_pos.y;

    let rows = conn.execute(UPDATE_WORKER_DEST_QRY, &[w_id, dest_x, dest_y],)?;
    info!(logger, "Worker_id {} destination updated", worker_id);
    
    Ok(())
}

/// Get the positions of all workers
pub fn get_all_worker_positions(conn : &Connection) -> Result<Vec<(i64, String, f64, f64)>, WorkerError> {
    let mut stmt = conn.prepare(SELECT_ALL_WORKERS_POS_QRY)?;
    let results_iter = stmt.query_map(NO_PARAMS, |row| {
        let w_id : i64 = row.get(0);
        let name : String = row.get(1);
        let x : f64 = row.get(2);
        let y : f64 = row.get(3);
        (w_id, name, x, y)
    })?;

    let mut results : Vec<(i64, String, f64, f64)> = Vec::new();
    for r in results_iter {
        let r = r?;
        results.push((r.0, r.1, r.2, r.3));
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    extern crate chrono;
    extern crate rand;
    extern crate slog;

    use super::*;
    use self::chrono::prelude::*;
    use std::path::{PathBuf, Path};
    use std::fs;
    use self::rand::Rng;
    use std::time::Duration;
    use logging;
    use slog::Drain;

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
        let logger = logging::create_discard_logger();

        let _conn = get_db_connection(&path, &logger).expect("Could not create DB file");
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
        let logger = logging::create_discard_logger();

        let conn = get_db_connection(&path, &logger).expect("Could not create DB file");
        let _res = create_db_objects(&conn, &logger).expect("Could not create DB tables");

        let mut stmt = conn.prepare(QRY_TABLE_EXISTS).expect("Could not prepare statement");
        let mut row_id : i64 = -1;
        row_id = stmt.query_row(&["workers"], |row| row.get(0)).expect("Could not execute query");
        assert_ne!(row_id, -1);

        row_id = stmt.query_row(&["worker_positions"], |row| row.get(0)).expect("Could not execute query");
        assert_ne!(row_id, 0);

        row_id = stmt.query_row(&["worker_velocities"], |row| row.get(0)).expect("Could not execute query");
        assert_ne!(row_id, 0);

        row_id = stmt.query_row(&["worker_destinations"], |row| row.get(0)).expect("Could not execute query");
        assert_ne!(row_id, 0);
    }

    //Unit test for confirming a worker is registered in the db
    #[test]
    fn test_register_worker() {
        //Add random wait to avoid collitions on the same DB file
        let mut rng = rand::thread_rng();
        let wait : u64 = rng.gen::<u64>() % 100;
        std::thread::sleep(Duration::from_millis(wait));
        
        let path = create_test_dir("mobility");
        let logger = logging::create_discard_logger();

        info!(logger, "Test results placed in {}", &path);

        let conn = get_db_connection(&path, &logger).expect("Could not create DB file");

        let _res = create_db_objects(&conn, &logger).expect("Could not create positions table");
        let name = String::from("worker1");
        let id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary id
        let pos = Position{ x : 0.0, y : 0.0 };
        let vel = Velocity{ x : 0.0, y : 0.0 };
        let sr = String::from("/tmp/sr.socket");
        let lr = String::from("/tmp/lr.socket");

        let worker_db_id = register_worker(&conn, 
                                           name, 
                                           &id, 
                                           &pos, 
                                           &vel, 
                                           &None, 
                                           Some(sr), 
                                           Some(lr),
                                           &logger).expect("Could not register worker");
        
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
        let logger = logging::create_discard_logger();

        info!(logger, "Test results placed in {}", &path);

        let conn = get_db_connection(&path, &logger).expect("Could not create DB file");
        let _res = create_db_objects(&conn, &logger).expect("Could not create positions table");

        //Create test worker1
        let source_name = String::from("worker1");
        let source_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary id
        let source_pos = Position{ x : 0.0, y : 0.0 };
        let vel = Velocity{ x : 0.0, y : 0.0 };
        let source_sr = String::from("/tmp/sr_1.socket");
        let source_lr = String::from("/tmp/lr_1.socket");
        let _id = register_worker(&conn, 
                                  source_name, 
                                  &source_id, 
                                  &source_pos, 
                                  &vel, 
                                  &None, 
                                  Some(source_sr), 
                                  Some(source_lr),
                                  &logger).expect("Could not register worker1");

        //Create test worker2
        //This worker should be in range.
        let name = String::from("worker2");
        let id = String::from("416d77337e24399dc7a5aa058039f72b");
        let pos = Position{ x : 50.0, y : -25.50 };
        let sr = String::from("/tmp/sr_2.socket");
        let lr = String::from("/tmp/lr_2.socket");
        let _id = register_worker(&conn, 
                                  name, 
                                  &id, 
                                  &pos, 
                                  &vel, 
                                  &None, 
                                  Some(sr), 
                                  Some(lr),
                                  &logger).expect("Could not register worker2");

        //Create test worker3
        //This worker should be in range.
        let name = String::from("worker3");
        let id = String::from("416d77337e24399dc7a5aa058039f72c");
        let pos = Position{ x : 30.0, y : 45.0 };
        let sr = String::from("/tmp/sr_3.socket");
        let lr = String::from("/tmp/lr_3.socket");
        let _id = register_worker(&conn, 
                                  name, 
                                  &id, 
                                  &pos, 
                                  &vel, 
                                  &None, 
                                  Some(sr), 
                                  Some(lr),
                                  &logger).expect("Could not register worker3");

        //Create test worker4
        //This worker should NOT be in range.
        let name = String::from("worker4");
        let id = String::from("416d77337e24399dc7a5aa058039f72d");
        let pos = Position{ x : 120.0, y : 0.0 };
        let sr = String::from("/tmp/sr_4.socket");
        let lr = String::from("/tmp/lr_4.socket");
        let _id = register_worker(&conn, 
                                  name, 
                                  &id, 
                                  &pos, 
                                  &vel, 
                                  &None, 
                                  Some(sr), 
                                  Some(lr),
                                  &logger).expect("Could not register worker4");

        //Create test worker5
        //This worker should NOT be in range.
        let name = String::from("worker5");
        let id = String::from("416d77337e24399dc7a5aa058039f72e");
        let pos = Position{ x : 60.0, y : 90.0 };
        let sr = String::from("/tmp/sr_5.socket");
        let lr = String::from("/tmp/lr_5.socket");
        let _id = register_worker(&conn, 
                                  name, 
                                  &id, 
                                  &pos, 
                                  &vel, 
                                  &None, 
                                  Some(sr), 
                                  Some(lr),
                                  &logger).expect("Could not register worker5");

        let workers_in_range : Vec<Peer> = get_workers_in_range(&conn, &source_id, 100.0).unwrap();
        println!("{:?}", &workers_in_range);
        assert_eq!(workers_in_range.len(), 2);
    }

    //Unit test for checking peers are within range.
    #[test]
    fn test_workers_mobility() {
        use std::collections::HashMap;

        let path = create_test_dir("basic_mobility");
        let logger = logging::create_discard_logger();
        info!(logger, "Test results placed in {}", &path);

        let conn = get_db_connection(&path, &logger).expect("Could not create DB file");
        let _res = create_db_objects(&conn, &logger).expect("Could not create positions table");

        //Create test worker1
        let source_name = String::from("worker1");
        let source_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary id
        let source_pos = Position{ x : 0.0, y : 0.0 };
        let vel1 = Velocity{ x : 1.0, y : 0.0 };
        let source_sr = String::from("/tmp/sr_1.socket");
        let source_lr = String::from("/tmp/lr_1.socket");
        let _id = register_worker(&conn, 
                                  source_name, 
                                  &source_id, 
                                  &source_pos, 
                                  &vel1, 
                                  &None, 
                                  Some(source_sr), 
                                  Some(source_lr),
                                  &logger).expect("Could not register worker1");

        //Create test worker2
        //This worker should be in range.
        let name = String::from("worker2");
        let id = String::from("416d77337e24399dc7a5aa058039f72b");
        let pos = Position{ x : 50.0, y : -25.50 };
        let vel2 = Velocity{ x : 1.0, y : -1.5 };
        let sr = String::from("/tmp/sr_2.socket");
        let lr = String::from("/tmp/lr_2.socket");
        let _id = register_worker(&conn, 
                                  name, 
                                  &id, 
                                  &pos, 
                                  &vel2, 
                                  &None, 
                                  Some(sr), 
                                  Some(lr),
                                  &logger).expect("Could not register worker2");

        //Create test worker3
        //This worker should be in range.
        let name = String::from("worker3");
        let id = String::from("416d77337e24399dc7a5aa058039f72c");
        let pos = Position{ x : 30.0, y : 45.0 };
        let vel3 = Velocity{ x : -3.0, y : 1.5 };
        let sr = String::from("/tmp/sr_3.socket");
        let lr = String::from("/tmp/lr_3.socket");
        let _id = register_worker(&conn, 
                                  name, 
                                  &id, 
                                  &pos, 
                                  &vel3, 
                                  &None, 
                                  Some(sr), 
                                  Some(lr),
                                  &logger).expect("Could not register worker3");

        let rows = update_worker_positions(&conn).expect("Could not update worker positions");
        assert_eq!(rows, 3);

        let SELECT_POSITIONS_QRY = "
        SELECT Workers.Worker_ID,
               Worker_positions.X,
               Worker_positions.Y
        FROM Workers
        JOIN Worker_positions on Workers.ID = Worker_positions.worker_id";
        let mut stmt = conn.prepare(SELECT_POSITIONS_QRY).expect("Could not prepare statement");
        let pos_iter = stmt.query_map(NO_PARAMS, |row| {
            let wid = row.get(0);
            let x = row.get(1);
            let y = row.get(2);
            (wid, x, y)    
        }).expect("Could not execute query");
        
        let mut positions = HashMap::new();
        for p in pos_iter {
            let (w, x, y) : (String, f64, f64)= p.unwrap();
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
    }
}