use crate::backend::{stop_all_workers, update_worker_vel, select_workers_at_destination, stop_workers, update_worker_target};
use crate::*;
use crate::master::test_specification::Area;
use rand::SeedableRng;
use diesel::pg::PgConnection;
use slog::Logger;
use std::collections::HashMap;
use rand::{rngs::StdRng, Rng};
use rand::distributions::Uniform;
use rand_distr::Normal;
use chrono::{DateTime, Utc};
use slog::{Key, Value, Record, Serializer};
use std::num::ParseFloatError;
use std::str::FromStr;

/// The update period for the mobility thread
pub const DEFAULT_MOBILITY_PERIOD: u64 = ONE_SECOND_NS;

///Struct to encapsule the 2D position of the worker
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Default, Copy)]
pub struct Position {
    /// X component
    pub x: f64,
    /// Y component
    pub y: f64,
}

impl Eq for Position { }

impl Position {
    pub fn distance(&self, other: &Position) -> f64 {
        euclidean_distance(self.x, self.y, other.x, other.y)
    }
}

impl FromStr for Position {
    type Err = ParseFloatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let coords: Vec<&str> = s.trim_matches(|p| p == '(' || p == ')' || p == '"')
                                 .split(',')
                                 .collect();

        let x_fromstr = coords[0].parse::<f64>()?;
        let y_fromstr = coords[1].parse::<f64>()?;

        Ok(Position { x: x_fromstr, y: y_fromstr })
    }
}

impl Value for Position {
    fn serialize(&self, _rec: &Record, key: Key, serializer: &mut dyn Serializer) -> slog::Result {
        let val = format!("({},{})", self.x, self.y);
        serializer.emit_str(key, &val)
    }
}

///Struct to encapsule the velocity vector of a worker
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy, Default)]
pub struct Velocity {
    /// X component
    pub x: f64,
    /// Y component
    pub y: f64,
}

impl Velocity {
    pub fn magnitude(&self) -> f64 {
        (self.x.powi(2) + self.y.powi(2)).sqrt()
    }
}

impl FromStr for Velocity {
    type Err = ParseFloatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let coords: Vec<&str> = s.trim_matches(|p| p == '(' || p == ')' || p == '"')
                                 .split(',')
                                 .collect();

        let x_fromstr = coords[0].parse::<f64>()?;
        let y_fromstr = coords[1].parse::<f64>()?;

        Ok(Velocity { x: x_fromstr, y: y_fromstr })
    }
}

impl Value for Velocity {
    fn serialize(&self, _rec: &Record, key: Key, serializer: &mut dyn Serializer) -> slog::Result {
        let val = format!("({},{})", self.x, self.y);
        serializer.emit_str(key, &val)
    }
}

/// Struct representing the state of a node at some point in the simulation
#[derive(Debug,)]
pub struct NodeState {
    /// Database id of the node
    pub id: i32,
    /// Name of the node,
    pub name: String,
    /// Current position of the node
    pub pos: Position,
    /// Current velocity of the node
    pub vel: Velocity,
    /// Current target destination for the node
    pub dest: Option<Position>,
}

pub type NodeMobilityState = (Position, Position, Velocity);
pub trait MobilityHandler {
    fn handle_iteration(&mut self, ) -> Result<Vec<NodeState>, MeshSimError>;

    fn get_mobility_period(&self) -> u64;
}

// Stationary mobility model
pub struct Stationary {
    period: u64
}

impl Stationary {
    pub fn new(conn: &PgConnection, logger: &Logger, period: Option<u64>) -> Self {
        info!(logger, "Stationary mobility model selected. Stopping all nodes.");
        let _rows = match stop_all_workers(conn) {
            Ok(rows) => rows,
            Err(e) => {
                error!(logger, "{}", e);
                0
            }
        };
        Stationary {
            period: period.unwrap_or(DEFAULT_MOBILITY_PERIOD)
        }
    }
}

impl MobilityHandler for Stationary {
    fn handle_iteration(&mut self, ) -> Result<Vec<NodeState>, MeshSimError> {
        Ok(Vec::new())
    }

    fn get_mobility_period(&self) -> u64 {
        return self.period;
    }
}

// Random Waypoint mobility model
pub struct RandomWaypoint {
    paused_workers: HashMap<i32, PendingWorker>,
    velocity_distribution: Normal<f64>,
    area_width_dist: Uniform<f64>,
    area_height_dist: Uniform<f64>,
    pause_time: i64,
    conn: PgConnection,
    rng: StdRng,
    logger: Logger,
    period: u64
}

type PendingWorker = (DateTime<Utc>, Velocity);

impl RandomWaypoint {

    pub fn new(
        vel_mean: f64,
        vel_std: f64,
        random_seed: u64,
        simulation_area: Area,
        pause_time: i64,
        conn: PgConnection,
        logger: Logger,
        period: Option<u64>) -> Result<Self, MeshSimError> {
        let paused_workers: HashMap<i32, PendingWorker> = HashMap::new();
        let rng = StdRng::seed_from_u64(random_seed);
        let velocity_distribution = Normal::new(vel_mean, vel_std)
        .map_err(|e| { 
            let err_msg = format!("Could not create Normal distribution from Mean:{} & SD:{}", vel_mean, vel_std);
            MeshSimError{
                kind: MeshSimErrorKind::Configuration(err_msg),
                cause: Some(Box::new(e))
            }
        })?;
        let width_dist = Uniform::new_inclusive(0., simulation_area.width);
        let height_dist = Uniform::new_inclusive(0., simulation_area.height);

        let rwp = RandomWaypoint {
            paused_workers,
            velocity_distribution,
            area_width_dist: width_dist,
            area_height_dist: height_dist,
            pause_time,
            conn,
            rng,
            logger,
            period: period.unwrap_or(DEFAULT_MOBILITY_PERIOD),
        };
        Ok(rwp)
    }
}

impl MobilityHandler for RandomWaypoint {

    fn handle_iteration(&mut self,) -> Result<Vec<NodeState>, MeshSimError> {
        // Ratio at which the mobility model is updated, based on update period
        let ratio = self.period as f64 / ONE_SECOND_NS as f64;
        //Select workers that reached their destination
        let workers = select_workers_at_destination(&self.conn, ratio)?;

        // Filther those nodes at their destination that were procssed before and remain there because they are paused or about to be restarted.
        let newly_arrived: Vec<NodeState> = workers.into_iter()
        .filter(|n| !self.paused_workers.contains_key(&n.id))
        .collect();
        //Get only the the ids of the newly arrived to stop them in batch.
        let na_ids: Vec<i32> = newly_arrived.iter()
        .map(|n| n.id)
        .collect();
        //Stop the newly arrived workers. Skip this step if the pause time is zero.
        if self.pause_time > 0 {
            let _rows = stop_workers(&self.conn, &na_ids, &self.logger)?;
        }
        
        //Now calculate a new destination and velocity for all the newly arrived workers, and add them to the paused list
        for w in newly_arrived.iter() {
            //Calculate parameters
            let next_x: f64 = self.rng.sample(self.area_width_dist);
            let next_y: f64 = self.rng.sample(self.area_height_dist);
            let vel: f64 = self.rng.sample(self.velocity_distribution);
            let distance: f64 = euclidean_distance(w.pos.x, w.pos.y, next_x, next_y);
            let time: f64 = distance / vel;
            let x_vel = (next_x - w.pos.x) / time;
            let y_vel = (next_y - w.pos.y) / time;

            //Update worker target position
            let _res = update_worker_target(
                &self.conn,
                Position {
                    x: next_x,
                    y: next_y,
                },
                w.id,
                &self.logger,
            );

            let restart_time = Utc::now() + chrono::Duration::nanoseconds(self.pause_time * ONE_MILLISECOND_NS as i64);
            self.paused_workers.insert(w.id, (restart_time, Velocity { x: x_vel, y: y_vel }));
        }

        // Restart movement for workers whose pause has ended.
        let restarted_workers: Vec<i32> = self.paused_workers
        .iter()
        //Filter those whose pause time has expired
        .filter(|(_w_id, &(ts, _vel))| ts <= Utc::now())
        //Update the velocity for the workers, and collect their ids
        .map(|(&w_id, &(_ts, vel))| { 
            let _ = update_worker_vel(&self.conn, vel, w_id, &self.logger)?;
            Ok(w_id)
        })
        .filter_map(|v: Result<i32, MeshSimError>| v.ok())
        .collect();

        //Remove the workers that were succesfully updated from the pending list
        self.paused_workers.retain(|&k, _| !restarted_workers.contains(&k));
        Ok(newly_arrived)
    }

    fn get_mobility_period(&self) -> u64 {
        return self.period;
    }
}

pub fn euclidean_distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    let xs = (x2 - x1).powf(2.0);
    let ys = (y2 - y1).powf(2.0);
    (xs + ys).sqrt()
}

// Increased mobility model
pub struct IncreasedMobility {
    paused_workers: HashMap<i32, PendingWorker>,
    velocity_increase: f64,
    area_width_dist: Uniform<f64>,
    area_height_dist: Uniform<f64>,
    pause_time: i64,
    conn: PgConnection,
    rng: StdRng,
    logger: Logger,
    period: u64,
}

impl IncreasedMobility {
    pub fn new(
        vel_incr: f64,
        random_seed: u64,
        simulation_area: Area,
        pause_time: i64,
        conn: PgConnection,
        logger: Logger,
        period: Option<u64>) -> Self {
        let paused_workers: HashMap<i32, PendingWorker> = HashMap::new();
        let rng = StdRng::seed_from_u64(random_seed);
        let width_dist = Uniform::new_inclusive(0., simulation_area.width);
        let height_dist = Uniform::new_inclusive(0., simulation_area.height);

        IncreasedMobility {
            paused_workers,
            velocity_increase: vel_incr,
            area_width_dist: width_dist,
            area_height_dist: height_dist,
            pause_time,
            conn,
            rng,
            logger,
            period: period.unwrap_or(DEFAULT_MOBILITY_PERIOD),
        }
    }
}

impl MobilityHandler for IncreasedMobility {
    fn handle_iteration(&mut self,) -> Result<Vec<NodeState>, MeshSimError> {
        // Ratio at which the mobility model is updated, based on update period
        let ratio = self.period as f64 / ONE_SECOND_NS as f64;
        //Select workers that reached their destination
        let workers = select_workers_at_destination(&self.conn, ratio)?;

        // Filther those nodes at their destination that were procssed before and remain there because they are paused or about to be restarted.
        let newly_arrived: Vec<NodeState> = workers.into_iter()
        .filter(|n| !self.paused_workers.contains_key(&n.id))
        .collect();
        //Get only the the ids of the newly arrived to stop them in batch.
        let na_ids: Vec<i32> = newly_arrived.iter()
        .map(|n| n.id)
        .collect();
        //Stop the newly arrived workers. Skip this step if the pause time is zero.
        if self.pause_time > 0 {
            let _rows = stop_workers(&self.conn, &na_ids, &self.logger)?;
        }
        
        //Now calculate a new destination and velocity for all the newly arrived workers, and add them to the paused list
        for w in newly_arrived.iter() {
            //Calculate parameters
            let next_x: f64 = self.rng.sample(self.area_width_dist);
            let next_y: f64 = self.rng.sample(self.area_height_dist);

            let new_vel = (w.vel.magnitude() * self.velocity_increase) + w.vel.magnitude();
            let distance: f64 = euclidean_distance(w.pos.x, w.pos.y, next_x, next_y);
            let time: f64 = distance / new_vel;
            let x_vel = (next_x - w.pos.x) / time;
            let y_vel = (next_y - w.pos.y) / time;

            //Update worker target position
            let _res = update_worker_target(
                &self.conn,
                Position {
                    x: next_x,
                    y: next_y,
                },
                w.id,
                &self.logger,
            );

            let restart_time = Utc::now() + chrono::Duration::milliseconds(self.pause_time);
            self.paused_workers.insert(w.id, (restart_time, Velocity { x: x_vel, y: y_vel }));
        }

        // Restart movement for workers whose pause has ended.
        let restarted_workers: Vec<i32> = self.paused_workers
        .iter()
        //Filter those whose pause time has expired
        .filter(|(_w_id, &(ts, _vel))| ts <= Utc::now())
        //Update the velocity for the workers, and collect their ids
        .map(|(&w_id, &(_ts, vel))| { 
            let _ = update_worker_vel(&self.conn, vel, w_id, &self.logger)?;
            Ok(w_id)
        })
        .filter_map(|v: Result<i32, MeshSimError>| v.ok())
        .collect();

        //Remove the workers that were succesfully updated from the pending list
        self.paused_workers.retain(|&k, _| !restarted_workers.contains(&k));

        Ok(newly_arrived)
    }

    fn get_mobility_period(&self) -> u64 {
        return self.period;
    }
}