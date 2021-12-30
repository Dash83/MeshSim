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
}

// How far workers move in one iteration
pub const STEP_DISTANCE: f64 = 10.0;
// Total distance workers move to reach their destination
pub const TOTAL_DISTANCE: f64 = 100.0;
// Size of the test area
pub const MOBILITY_TEST_AREA: Area = Area{width: TOTAL_DISTANCE, height: TOTAL_DISTANCE};

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
    pub fn generate_test_workers_basic() -> Vec<MobilityTestWorker> {
        let mut test_workers = Vec::new();

        // This worker moves forward horizontally
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_FRWD_X"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: TOTAL_DISTANCE, y: 0.0 },
            velocity: Velocity { x: STEP_DISTANCE, y: 0.0 },
        });

        // This worker moves upwards vertically
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_UP_Y"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: 0.0, y: TOTAL_DISTANCE },
            velocity: Velocity { x: 0.0, y: STEP_DISTANCE },
        });

        // This worker moves backwards horizontally
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_BACK_X"),
            initial_position: Position { x: TOTAL_DISTANCE, y: 0.0 },
            previous_position: Position { x: TOTAL_DISTANCE, y: 0.0 },
            destination: Position { x: 0.0, y: 0.0 },
            velocity: Velocity { x: (STEP_DISTANCE * -1.0), y: 0.0 },
        });

        // This worker moves downwards vertically
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_DOWN_Y"),
            initial_position: Position { x: 0.0, y: TOTAL_DISTANCE },
            previous_position: Position { x: 0.0, y: TOTAL_DISTANCE },
            destination: Position { x: 0.0, y: 0.0 },
            velocity: Velocity { x: 0.0, y: (STEP_DISTANCE * -1.0) },
        });
        
        // This worker moves forward diagonally
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_FRWD_D"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: TOTAL_DISTANCE, y: TOTAL_DISTANCE },
            velocity: Velocity { x: STEP_DISTANCE, y: STEP_DISTANCE },
        });
        
        // This worker moves backwards diagonally
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_BACK_D"),
            initial_position: Position { x: TOTAL_DISTANCE, y: TOTAL_DISTANCE },
            previous_position: Position { x: TOTAL_DISTANCE, y: TOTAL_DISTANCE },
            destination: Position { x: 0.0, y: 0.0 },
            velocity: Velocity { x: (STEP_DISTANCE * -1.0), y: (STEP_DISTANCE * -1.0) },
        });

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates.
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_FRWD_X_NEAR"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: (TOTAL_DISTANCE - STEP_DISTANCE), y: 0.0 },
            velocity: Velocity { x: (STEP_DISTANCE - 0.1), y: 0.0 },
        });

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates, while moving
        // backwards horizontally.
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_BACK_X_NEAR"),
            initial_position: Position { x: (TOTAL_DISTANCE - STEP_DISTANCE), y: 0.0 },
            previous_position: Position { x: (TOTAL_DISTANCE - STEP_DISTANCE), y: 0.0 },
            destination: Position { x: 0.0, y: 0.0 },
            velocity: Velocity { x: ((STEP_DISTANCE - 0.1) * -1.0), y: 0.0 },
        });

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates, while moving
        // upwards diagonally.
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_FRWD_D_NEAR"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: (TOTAL_DISTANCE - STEP_DISTANCE), y: (TOTAL_DISTANCE - STEP_DISTANCE) },
            velocity: Velocity { x: (STEP_DISTANCE - 0.1), y: (STEP_DISTANCE - 0.1) },
        });

        return test_workers;
    }

    /// Generates a list of workers to test special cases in mobility tests.
    // pub fn generate_test_workers_special() -> Vec<MobilityTestWorker> {
    //     let mut test_workers = Vec::new();
        
    //     // This worker will not move
    //     test_workers.push(MobilityTestWorker {
    //         name: String::from("WORKER_STATIC_AT_DEST"),
    //         initial_position: Position { x: 0.0, y: 0.0 },
    //         previous_position: Position { x: 0.0, y: 0.0 },
    //         destination: Position { x: 0.0, y: 0.0 },
    //         velocity: Velocity { x: 0.0, y: 0.0 },
    //     });

    //     // This worker will not move and will never reach its destination
    //     test_workers.push(MobilityTestWorker {
    //         name: String::from("WORKER_STATIC"),
    //         initial_position: Position { x: 0.0, y: 0.0 },
    //         previous_position: Position { x: 0.0, y: 0.0 },
    //         destination: Position { x: TOTAL_DISTANCE, y: TOTAL_DISTANCE },
    //         velocity: Velocity { x: 0.0, y: 0.0 },
    //     });

    //     return test_workers;
    // }

    /// Adds the worker to the mobility system
    pub fn add_to_mobility_system(&self, data : &TestSetup, conn: &PgConnection) {
        let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
        let work_dir = data.work_dir.clone();
        let short_radio_range = 100.0;
        let long_radio_range = 500.0;
        let random_seed = 1;

        // Create the short and long radio configurations
        let mut sr_config = RadioConfig::new();
        sr_config.range = short_radio_range;
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
        lr_config.range = long_radio_range;
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

}

/// Calculates the update ration given the mobility period in nanoseconds.
/// All velocities are expresed in meters per second (see start_mobility_thread).
/// The period is is the time elapsed in one call to update_worker_positions().
pub fn mobility_test_period_to_ratio(period: u64) -> f64 {
    return period as f64 / mesh_simulator::ONE_SECOND_NS as f64;
}
