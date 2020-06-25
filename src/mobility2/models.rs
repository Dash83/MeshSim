//! Definition of all the structs used to load/store into Postgres
//!
use super::schema::*;

/// Rows stored in the workers table
#[derive(Insertable)]
#[table_name = "workers"]
pub struct NewWorker<'a> {
    pub worker_name: &'a str,
    pub worker_id: &'a str,
    pub short_range_address: Option<String>,
    pub long_range_address: Option<String>,
}

#[derive(Debug, Queryable, QueryableByName)]
#[table_name = "workers"]
pub struct WorkerRecord {
    pub id: i32,
    pub worker_name: String,
    pub worker_id: String,
    pub short_range_address: Option<String>,
    pub long_range_address: Option<String>,
}

/// Rows stored in the worker_positions table
#[derive(Debug, Insertable, AsChangeset, Queryable)]
#[table_name = "worker_positions"]
pub struct NewPos {
    pub worker_id: i32,
    pub x: f64,
    pub y: f64,
}

/// Rows stored in the worker_positions table
#[derive(Insertable, AsChangeset)]
#[table_name = "worker_destinations"]
pub struct NewDest {
    pub worker_id: i32,
    pub x: f64,
    pub y: f64,
}

/// Rows stored in the worker_velocities table
#[derive(Insertable, AsChangeset)]
#[table_name = "worker_velocities"]
pub struct NewVel {
    pub worker_id: i32,
    pub x: f64,
    pub y: f64,
}

/// Rows stored in the worker_velocities table
#[derive(Insertable)]
#[table_name = "active_wifi_transmitters"]
pub struct WifiTransmitter {
    pub worker_id: i32,
}
