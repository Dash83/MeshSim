use std::collections::HashMap;

use diesel::PgConnection;
use mesh_simulator::{mobility::{Position, Velocity}, tests::common::TestSetup, worker::{worker_config::RadioConfig, OperationMode, radio::RadioTypes}, backend::register_worker, master::test_specification::Area};

extern crate mesh_simulator;

/// Helper worker structure
pub struct MobilityTestWorker {
    pub name: String,
    pub initial_position: Position,
    pub previous_position: Position,
    pub destination: Position,
    pub velocity: Velocity,
    pub short_radio_range: f64,
    pub long_radio_range: f64,
}

// Size of the test area
pub const MOBILITY_TEST_AREA: Area = Area{width: 100.0, height: 100.0};

pub const SHORT_RADIO_RANGE_DEFAULT: f64 = 100.0;
pub const LONG_RADIO_RANGE_DEFAULT: f64 = 500.0;

impl MobilityTestWorker {

    /// Gets the position the worker is expected to move in the next call to
    /// update_worker_positions, given its previous position and velocity.
    pub fn get_next_position(&self, period: Option<u64>) -> Position {
        // All velocities are expresed in meters per second (see start_mobility_thread).
        // This is the time period in elapsed in one call to update_worker_positions().
        let ratio = mobility_test_period_to_ratio(period.unwrap_or(mesh_simulator::mobility::DEFAULT_MOBILITY_PERIOD));
        return Position {
            x: self.previous_position.x + (self.velocity.x * ratio),
            y: self.previous_position.y + (self.velocity.y * ratio)
        };
    }

    /// Returns TRUE if the worker is expected to arrive at its destination
    /// in the next step, i.e. the next call to update_worker_positions().
    pub fn is_next_at_destination(&self, period: Option<u64>) -> bool {
        let next_position = self.get_next_position(period);
        let ratio = mobility_test_period_to_ratio(period.unwrap_or(mesh_simulator::mobility::DEFAULT_MOBILITY_PERIOD));

        // The mobility model will report the node as being at the
        // destination if it is close enough to the destination,
        // where 'close' means at a distance smaller than one hop past
        // the destination but not before.
        return  self.destination.eq(&next_position) ||
                (self.destination.distance(&self.previous_position) > 0.0 &&
                 self.destination.distance(&self.previous_position) < (self.velocity.magnitude() * ratio) &&
                 self.destination.distance(&next_position)          < (self.velocity.magnitude() * ratio));
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
            Some(String::from("WORKER_FRWD_X")),
            None,
            None,
            None,
            Some(Velocity { x: step_distance, y: 0.0 }),
            Some(Position { x: total_distance, y: 0.0 })));

        // This worker moves upwards vertically
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_UP_Y")),
            None,
            None,
            None,
            Some(Velocity { x: 0.0, y: step_distance }),
            Some(Position { x: 0.0, y: total_distance })));

        // This worker moves backwards horizontally
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_BACK_X")),
            None,
            None,
            Some(Position { x: total_distance, y: 0.0 }),
            Some(Velocity { x: (step_distance * -1.0), y: 0.0 }),
            None));

        // This worker moves downwards vertically
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_DOWN_Y")),
            None,
            None,
            Some(Position { x: 0.0, y: total_distance }),
            Some(Velocity { x: 0.0, y: (step_distance * -1.0) }),
            None));
        
        // This worker moves forward diagonally
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_FRWD_D")),
            None,
            None,
            None,
            Some(Velocity { x: step_distance, y: step_distance }),
            Some(Position { x: total_distance, y: total_distance })));
        
        // This worker moves backwards diagonally
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_BACK_D")),
            None,
            None,
            Some(Position { x: total_distance, y: total_distance }),
            Some(Velocity { x: (step_distance * -1.0), y: (step_distance * -1.0) }),
            None));

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates.
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_FRWD_X_NEAR")),
            None,
            None,
            None,
            Some(Velocity { x: (step_distance - 0.1), y: 0.0 }),
            Some(Position { x: (total_distance - step_distance), y: 0.0 })));

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates, while moving
        // backwards horizontally.
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_BACK_X_NEAR")),
            None,
            None,
            Some(Position { x: (total_distance - step_distance), y: 0.0 }),
            Some(Velocity { x: ((step_distance - 0.1) * -1.0), y: 0.0 }),
            None));

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates, while moving
        // upwards diagonally.
        test_workers.push(add_worker_to_test(data, conn,
            Some(String::from("WORKER_FRWD_D_NEAR")),
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
                self.initial_position,
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
        initial_position:   pos.unwrap_or(Position { x: 0.0, y: 0.0 }),
        previous_position:  pos.unwrap_or(Position { x: 0.0, y: 0.0 }),
        destination:        dest.unwrap_or(Position { x: 0.0, y: 0.0 }),
        velocity:           vel.unwrap_or(Velocity { x: 0.0, y: 0.0 }),
        short_radio_range:  sr_range.unwrap_or(SHORT_RADIO_RANGE_DEFAULT),
        long_radio_range:   lr_range.unwrap_or(LONG_RADIO_RANGE_DEFAULT),
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
