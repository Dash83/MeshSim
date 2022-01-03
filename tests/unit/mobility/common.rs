use std::collections::HashMap;

use diesel::PgConnection;
use mesh_simulator::{mobility::{Position, Velocity, NodeState}, tests::common::TestSetup, worker::{worker_config::RadioConfig, OperationMode, radio::RadioTypes}, backend::{register_worker}, master::test_specification::Area};

extern crate mesh_simulator;

/// Helper worker structure
pub struct MobilityTestWorker {
    pub name: String,
    pub previous_position: Position,
    pub destination: Position,
    pub velocity: Velocity,
    pub short_radio_range: f64,
    pub long_radio_range: f64,
    pub paused: bool,
}

// Size of the test area
pub const MOBILITY_TEST_AREA: Area = Area{width: 100.0, height: 100.0};

pub const SHORT_RADIO_RANGE_DEFAULT: f64 = 100.0;
pub const LONG_RADIO_RANGE_DEFAULT: f64 = 500.0;

impl MobilityTestWorker {

    /// Sanity checks the worker's node state with respect to its previous
    /// position. A worker's behavior before reaching its destination is
    /// the same regardless of the mobility model.
    pub fn verify_state(&self, state: &NodeState, period: Option<u64>) {
        if self.paused {
            // Verify the worker did not move
            assert_eq!(state.pos, self.previous_position,
                "Worker {} moved from {},{} to {},{} when it should be paused.",
                self.name,
                self.previous_position.x, self.previous_position.y,
                state.pos.x, state.pos.y);
        }
        else {
            // All velocities are expresed in meters per second (see start_mobility_thread).
            // This is the time period in elapsed in one call to update_worker_positions().
            let ratio = mobility_test_period_to_ratio(
                period.unwrap_or(mesh_simulator::mobility::DEFAULT_MOBILITY_PERIOD));
            
            // Verify the worker's position
            let expected_position = Position {
                x: self.previous_position.x + (self.velocity.x * ratio),
                y: self.previous_position.y + (self.velocity.y * ratio)
            };
            assert_eq!(&state.pos, &expected_position,
                "Worker {} moved to {},{} but {},{} was expected based on previous position: {},{} velocity: {},{}. New velocity: {},{}.",
                self.name,
                state.pos.x, state.pos.y,
                expected_position.x, expected_position.y,
                self.previous_position.x, self.previous_position.y,
                self.velocity.x, self.velocity.y,
                state.vel.x, state.vel.y);
            
            // Verify the worker's destination and velocity.
            // Before reaching its destination, the worker must keep the same
            // velocity and destination regardless of the mobility model.
            if !self.is_at_destination(&state.pos, period) {
                assert_eq!(state.dest.unwrap(), self.destination,
                    "Worker {} has a new destination {},{} before reaching its original destination {},{}. Previous position: {},{} velocity: {},{}. New position: {},{} velocity: {},{}.",
                    self.name,
                    state.dest.unwrap().x, state.dest.unwrap().y,
                    self.destination.x, self.destination.y,
                    self.previous_position.x, self.previous_position.y,
                    self.velocity.x, self.velocity.y,
                    state.pos.x, state.pos.y,
                    state.vel.x, state.vel.y);
    
                assert_eq!(state.vel, self.velocity,
                    "Worker {} has a new velocity {},{} before reaching its original destination {},{}. Previous position: {},{} velocity: {},{}. New position: {},{}.",
                    self.name, state.vel.x, state.vel.y,
                    self.destination.x, self.destination.y,
                    self.previous_position.x, self.previous_position.y,
                    self.velocity.x, self.velocity.y,
                    state.pos.x, state.pos.y);
            }
        }
    }

    /// Checks whether this worker was correctly reported as newly arrived at
    /// its destination. The handle_iteration() method of the mobility model
    /// will return a node state for every worker considered to be at its
    /// destination. 
    pub fn verify_newly_arrived_state(&self, state: &NodeState, newly_arrived_state: Option<&NodeState>, period: Option<u64>) {
        // The worker is considered to have arrived at its destination in this
        // update if it is not paused, its new position is at the destination,
        // and it actually changed to a different position in this update. 
        if  !self.paused &&
            self.is_at_destination(&state.pos, period) &&
            self.previous_position.ne(&state.pos) {
            assert!(newly_arrived_state.is_some(),
                "Worker {} moved from {},{} to {},{} considered to be at or near destination {},{} but select_workers_at_destination did not return this worker.",
                self.name,
                self.previous_position.x, self.previous_position.y,
                state.pos.x, state.pos.y,
                self.destination.x, self.destination.y);
        }
        else {
            assert!(newly_arrived_state.is_none(),
                "Worker {} wrongly reported as newly arrived when it moved from {},{} to {},{}, with destination {},{}.",
                self.name,
                self.previous_position.x, self.previous_position.y,
                state.pos.x, state.pos.y,
                self.destination.x, self.destination.y);
        }
    }

    /// Validates a proposed destination for this worker.
    /// The destination must be within the simulation area.
    pub fn validate_new_destination(&self, new_destination: &Position) {
        assert!(
            new_destination.x <= MOBILITY_TEST_AREA.width &&
            new_destination.y <= MOBILITY_TEST_AREA.height,
            "New destination {},{} for worker {} is outside of the test area w:{} h:{}. Previous destination: {},{}.",
            new_destination.x, new_destination.y,
            self.name,
            MOBILITY_TEST_AREA.width, MOBILITY_TEST_AREA.height,
            self.destination.x, self.destination.y);
    }

    /// Validates a proposed new velocity for this worker.
    /// Validates the velocity vector actually leads to the new destination.
    /// For this we calculate the shortest distance between a line P1P2
    /// and a point Q.
    /// - The line represents the trayectory that the worker will follow
    ///   from its current position on to the next positions based on its
    ///   velocity. This is an infinite line with equation y = mx + b
    ///   calculated from two points: point P1 at the current position, and
    ///   point P2 at the next position x: pos.x + vel.x, y: pos.y + vel.y
    /// - The point Q is the destination
    /// If the distance between the line and the point is zero it means
    /// the line intersects the point, i.e. the worker's trayectory will
    /// eventually reach its destination.
    pub fn validate_new_velocity(&self, new_velocity: &Velocity, new_destination: &Position) {
        let p1 = self.previous_position;
        let p2 = Position{x: self.previous_position.x + new_velocity.x, y: self.previous_position.y + new_velocity.y};
        let q = new_destination;
        // Calculate the slope m = (y2 - y1) / (x2 - x1)
        let m = (p2.y - p1.y) / (p2.x - p1.x);
        // Rearrange the line equation y = mx + c to form Ax + By + C = 0
        // as -mx + 1y - c = 0
        let a = m * -1.0;
        let b = 1.0;
        let c = (m * p1.x) - (p1.y);
        // Calculate the distance between line P1->P2 to point Q(x0,y0) with
        // equation distance = | Ax0 + By0 + C | / √ (A*A + B*B)
        let distance = ((a * q.x) + (b * q.y) + (c)).abs() / ((a * a) + (b * b)).sqrt();
        // Again, we round to 3 decimal places for the assert to make up for
        // the loss of precision of f64 in rust.
        let distance_aprox = (distance * 1000.0).round() / 1000.0;
        assert_eq!(distance_aprox, 0.0, 
            "New velocity {},{} for worker {} not expected to reach destination {},{} from current position {},{}.",
            new_velocity.x, new_velocity.y,
            self.name,
            new_destination.x, new_destination.y,
            self.previous_position.x, self.previous_position.y);

    }

    /// Returns TRUE if the given position is the worker's destination,
    /// based on its previous position and velocity.
    pub fn is_at_destination(&self, pos: &Position, period: Option<u64>) -> bool {
        let ratio = mobility_test_period_to_ratio(period.unwrap_or(mesh_simulator::mobility::DEFAULT_MOBILITY_PERIOD));

        // The mobility model will report the node as being at the
        // destination if it is close enough to the destination,
        // where 'close' means at a distance smaller than one hop past
        // the destination but not before.
        return  self.destination.eq(&pos) ||
                (self.destination.distance(&self.previous_position) > 0.0 &&
                 self.destination.distance(&self.previous_position) < (self.velocity.magnitude() * ratio) &&
                 self.destination.distance(&pos)                    < (self.velocity.magnitude() * ratio));
    }

    pub fn get_steps_to_destination(&self, period: Option<u64>) -> u64 {
        // All velocities are expresed in meters per second (see start_mobility_thread).
        // This is the time period in elapsed in one call to update_worker_positions().
        let ratio = mobility_test_period_to_ratio(period.unwrap_or(mesh_simulator::mobility::DEFAULT_MOBILITY_PERIOD));
        let distance = self.previous_position.distance(&self.destination);

        return (distance / (self.velocity.magnitude() * ratio)).ceil() as u64;
    }

    /// Generates a list of workers to run basic mobility tests.
    /// Each worker moves in a different direction to test a different scenario
    /// and all reach their destination at the same time.
    pub fn add_basic_mobility_test_workers(data : &TestSetup, conn: &PgConnection) -> Vec<MobilityTestWorker> {
        let mut test_workers = Vec::new();
        // How far workers move in one iteration
        let step_distance = 10.0;
        // Total distance workers move to reach their destination
        let total_distance = MOBILITY_TEST_AREA.width;

        // This worker moves forward horizontally
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_EXACT_FRWD_X")),
            None,
            None,
            None,
            Some(Velocity { x: step_distance, y: 0.0 }),
            Some(Position { x: total_distance, y: 0.0 })));

        // This worker moves upwards vertically
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_EXACT_UP_Y")),
            None,
            None,
            None,
            Some(Velocity { x: 0.0, y: step_distance }),
            Some(Position { x: 0.0, y: total_distance })));

        // This worker moves backwards horizontally
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_EXACT_BACK_X")),
            None,
            None,
            Some(Position { x: total_distance, y: 0.0 }),
            Some(Velocity { x: (step_distance * -1.0), y: 0.0 }),
            None));

        // This worker moves downwards vertically
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_EXACT_DOWN_Y")),
            None,
            None,
            Some(Position { x: 0.0, y: total_distance }),
            Some(Velocity { x: 0.0, y: (step_distance * -1.0) }),
            None));
        
        // This worker moves forward diagonally
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_EXACT_FRWD_D")),
            None,
            None,
            None,
            Some(Velocity { x: step_distance, y: step_distance }),
            Some(Position { x: total_distance, y: total_distance })));
        
        // This worker moves backwards diagonally
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_EXACT_BACK_D")),
            None,
            None,
            Some(Position { x: total_distance, y: total_distance }),
            Some(Velocity { x: (step_distance * -1.0), y: (step_distance * -1.0) }),
            None));

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates.
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_NEAR_FRWD_X")),
            None,
            None,
            None,
            Some(Velocity { x: (step_distance - 0.1), y: 0.0 }),
            Some(Position { x: (total_distance - step_distance), y: 0.0 })));

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates, while moving
        // backwards horizontally.
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_NEAR_BACK_X")),
            None,
            None,
            Some(Position { x: (total_distance - step_distance), y: 0.0 }),
            Some(Velocity { x: ((step_distance - 0.1) * -1.0), y: 0.0 }),
            None));

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates, while moving
        // upwards diagonally.
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_NEAR_FRWD_D")),
            None,
            None,
            None,
            Some(Velocity { x: (step_distance - 0.1), y: (step_distance - 0.1) }),
            Some(Position { x: (total_distance - step_distance), y: (total_distance - step_distance) })));

        return test_workers;
    }

    /// Generates a list of workers to test special cases in mobility tests.
    // pub fn generate_test_workers_special() -> Vec<MobilityTestWorker> {
    //     let mut test_workers = Vec::new();
    //
    //     // This worker will not move
    //     test_workers.push(MobilityTestWorker {
    //         name: String::from("WORKER_STATIC_AT_DEST"),
    //         initial_position: Position { x: 0.0, y: 0.0 },
    //         previous_position: Position { x: 0.0, y: 0.0 },
    //         destination: Position { x: 0.0, y: 0.0 },
    //         velocity: Velocity { x: 0.0, y: 0.0 },
    //     });
    //
    //     // This worker will not move and will never reach its destination
    //     test_workers.push(MobilityTestWorker {
    //         name: String::from("WORKER_STATIC"),
    //         initial_position: Position { x: 0.0, y: 0.0 },
    //         previous_position: Position { x: 0.0, y: 0.0 },
    //         destination: Position { x: TOTAL_DISTANCE, y: TOTAL_DISTANCE },
    //         velocity: Velocity { x: 0.0, y: 0.0 },
    //     });
    //
    //     return test_workers;
    // }

    /// Adds the worker to the mobility system
    fn add_to_mobility_system(&self, data : &TestSetup, conn: &PgConnection) {
        let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
        let work_dir = data.work_dir.clone();
        let random_seed = 1;

        // Create the short and long radio configurations
        let mut sr_config = RadioConfig::new();
        sr_config.range = self.short_radio_range;
        let (r1, _l1) = sr_config.create_radio(
                OperationMode::Simulated,
                RadioTypes::ShortRange,
                work_dir.clone(),
                self.name.clone(),
                worker_id.clone(),
                random_seed,
                None,
                data.logger.clone(),
            )
            .expect("Could not create short-range radio");

        let mut lr_config = RadioConfig::new();
        lr_config.range = self.long_radio_range;
        let (r2, _l2) = lr_config.create_radio(
                OperationMode::Simulated,
                RadioTypes::LongRange,
                work_dir,
                self.name.clone(),
                worker_id.clone(),
                random_seed,
                None,
                data.logger.clone(),
            )
            .expect("Could not create long-range radio");

        // Register the worker in the mobility system 
        let _worker_db_id = register_worker(
                &conn,
                self.name.clone(),
                worker_id,
                self.previous_position,
                self.velocity,
                &Some(self.destination),
                Some(r1.get_address().into()),
                Some(r2.get_address().into()),
                &data.logger,
            )
            .expect("Could not register worker");
    }

    fn add_neighbour(&self, data : &TestSetup, conn: &PgConnection, name: Option<String>) -> Self {
        let name = name.unwrap_or_else(|| { 
            let n: usize = rand::random();
            format!("worker{}", n)
        });
        let sr_range = self.short_radio_range;
        let lr_range = self.long_radio_range;
        let min_distance = sr_range / 10.0;
        let max_distance = sr_range * 0.85;
        let dist: f64 = (rand::random::<f64>() % max_distance) + min_distance;
        let theta: f64 = rand::random::<f64>() % 90.0;
        let y = theta.sin() * dist;
        let x = theta.cos() * dist;
        let pos = Position{x, y};

        return add_worker_to_test(data, conn, Some(name), Some(sr_range), Some(lr_range), Some(pos), None, None);
    }

    pub fn add_neighbours(&self, data : &TestSetup, conn: &PgConnection, num: usize) -> HashMap<String, Self> {
        let mut workers = HashMap::with_capacity(num);
        for _ in 0..num {
            let w = self.add_neighbour(data, conn, None);
            workers.insert(w.name.clone(), w);
        }

        return workers;
    }
}

/// Adds a worker with the given properties to the mobility system.
pub fn add_worker_to_test(
    data : &TestSetup, 
    conn: &PgConnection,
    name: Option<String>,
    sr_range: Option<f64>,
    lr_range: Option<f64>,
    pos: Option<Position>,
    vel: Option<Velocity>,
    dest: Option<Position>) -> MobilityTestWorker {

    let test_worker = MobilityTestWorker {
        name:               name.unwrap_or(String::from("worker1")),
        previous_position:  pos.unwrap_or(Position { x: 0.0, y: 0.0 }),
        destination:        dest.unwrap_or(Position { x: 0.0, y: 0.0 }),
        velocity:           vel.unwrap_or(Velocity { x: 0.0, y: 0.0 }),
        short_radio_range:  sr_range.unwrap_or(SHORT_RADIO_RANGE_DEFAULT),
        long_radio_range:   lr_range.unwrap_or(LONG_RADIO_RANGE_DEFAULT),
        paused:             false,
    };
    
    test_worker.add_to_mobility_system(&data, &conn);

    return test_worker;
}

/// Calculates the update ration given the mobility period in nanoseconds.
/// All velocities are expresed in meters per second (see start_mobility_thread).
/// The period is is the time elapsed in one call to update_worker_positions().
pub fn mobility_test_period_to_ratio(period: u64) -> f64 {
    return period as f64 / mesh_simulator::ONE_SECOND_NS as f64;
}
