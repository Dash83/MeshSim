//! This module handles the position and mobility features of the worker
//! 
extern crate rusqlite;

use self::rusqlite::types::ToSql;
use self::rusqlite::{Connection, NO_PARAMS, OpenFlags};
use std::path::PathBuf;
use worker::WorkerError;

/// Name used for the worker positions DB
pub const DB_NAME : &'static str = "worker_positions.db";
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

#[cfg(test)]
mod tests {
    extern crate chrono;
    extern crate rand;

    use super::*;
    use super::super::worker_config::WorkerConfig;
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
        let expected_file = PathBuf::from(&format!("{}/{}", path, DB_NAME));

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
        println!("DB conn: {:?}", &conn);

        let _res = create_positions_db(&conn).expect("Could not create positions table");
        let name = String::from("worker1");
        let id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary id
        let pos = Position{ x : 0.0, y : 0.0 };

        let worker_db_id = register_worker(&conn, name, id, &pos).expect("Could not register worker");
        
        assert!(worker_db_id > 0);
    }

}