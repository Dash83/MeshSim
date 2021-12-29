use diesel::PgConnection;
use mesh_simulator::{mobility::{Position, Velocity, NodeState}, tests::common::TestSetup, worker::{worker_config::RadioConfig, OperationMode, radio::RadioTypes}, backend::register_worker, master::test_specification::Area};

extern crate mesh_simulator;

/// Helper worker structure
pub struct MobilityTestWorker {
    pub name: String,
    pub initial_position: Position,
    pub previous_position: Position,
    pub destination: Position,
    pub velocity: Velocity,
    pub paused: bool,
}

// How far workers move in one iteration
static STEP_DISTANCE: f64 = 10.0;
// Total distance workers move to reach their destination
static TOTAL_DISTANCE: f64 = 100.0;
// Size of the test area
pub static MOBILITY_TEST_AREA: Area = Area{width: TOTAL_DISTANCE, height: TOTAL_DISTANCE};

impl MobilityTestWorker {

    // Generates a list of test workers and adds them to the mobility system
    pub fn setup(data : &TestSetup, conn: &PgConnection) -> Vec<MobilityTestWorker> {
        // Generate a list of test workers
        let test_workers = MobilityTestWorker::generate_test_workers();

        // Add the workers to the mobility system
        for worker in test_workers.iter(){
            worker.add_to_mobility_system(&data, &conn);
        }

        return test_workers;
    }

    /// Gets the number of test iterations this worker is expected to take to reach its original destination.
    pub fn get_steps_to_destination(&self) -> i32 {
        if self.destination == self.initial_position {
            // Workers that start at their destination will "arrive" at their destination in the 1st iteration.
            return 1;
        } else if self.velocity.x == 0.0 && self.velocity.y == 0.0 {
            // Workers that don't move (worker8) will never reach their destination
            return i32::MAX;
        }

        // Calculated as ( <final_position> - <initial_position> ) / <velocity>
        let steps_in_x = ((self.destination.x - self.initial_position.x).abs() / self.velocity.x.abs()).ceil() as i32;
        let steps_in_y = ((self.destination.y - self.initial_position.y).abs() / self.velocity.y.abs()).ceil() as i32;
        return steps_in_x.max(steps_in_y);
    }

    /// Verifies the worker's current position with respect to its previous position.
    pub fn verify_move(&mut self, worker_state: &NodeState) {
        assert!(worker_state.name.eq(&self.name));

        assert!(worker_state.pos.x == (self.previous_position.x + worker_state.vel.x));
        assert!(worker_state.pos.y == (self.previous_position.y + worker_state.vel.y));

        // Save the current position as the previous position
        self.previous_position = worker_state.pos;
    }

    /// Verifies that the worker moved again after it paused when it reached its destination
    fn verify_restart_movement(&self, worker_state: &NodeState) {
        assert!(worker_state.name.eq(&self.name));

        // For workers that reached their destination before the pause, verify they have a new destination and velocity
        if self.previous_position == self.destination {
            assert!(worker_state.dest.unwrap() != self.destination);
            assert!(worker_state.vel != self.velocity);
        }
    }

    // Generates a list of test workers where each worker moves in a different direction to test a different scenario.
    fn generate_test_workers() -> Vec<MobilityTestWorker> {
        let mut test_workers = Vec::new();

        // This worker moves forward horizontally
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_FRWD_X"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: TOTAL_DISTANCE, y: 0.0 },
            velocity: Velocity { x: STEP_DISTANCE, y: 0.0 },
            paused: false
        });

        // This worker moves upwards vertically
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_UP_Y"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: 0.0, y: TOTAL_DISTANCE },
            velocity: Velocity { x: 0.0, y: STEP_DISTANCE },
            paused: false
        });

        // This worker moves backwards horizontally
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_BACK_X"),
            initial_position: Position { x: TOTAL_DISTANCE, y: 0.0 },
            previous_position: Position { x: TOTAL_DISTANCE, y: 0.0 },
            destination: Position { x: 0.0, y: 0.0 },
            velocity: Velocity { x: (STEP_DISTANCE * -1.0), y: 0.0 },
            paused: false
        });

        // This worker moves downwards vertically
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_DOWN_Y"),
            initial_position: Position { x: 0.0, y: TOTAL_DISTANCE },
            previous_position: Position { x: 0.0, y: TOTAL_DISTANCE },
            destination: Position { x: 0.0, y: 0.0 },
            velocity: Velocity { x: 0.0, y: (STEP_DISTANCE * -1.0) },
            paused: false
        });
        
        // This worker moves forward diagonally
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_FRWD_DIAG"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: TOTAL_DISTANCE, y: TOTAL_DISTANCE },
            velocity: Velocity { x: STEP_DISTANCE, y: STEP_DISTANCE },
            paused: false
        });
        
        // This worker moves backwards diagonally
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_BACK_DIAG"),
            initial_position: Position { x: TOTAL_DISTANCE, y: TOTAL_DISTANCE },
            previous_position: Position { x: TOTAL_DISTANCE, y: TOTAL_DISTANCE },
            destination: Position { x: 0.0, y: 0.0 },
            velocity: Velocity { x: (STEP_DISTANCE * -1.0), y: (STEP_DISTANCE * -1.0) },
            paused: false
        });
        
        // This worker will not move
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_STATIC_AT_DEST"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: 0.0, y: 0.0 },
            velocity: Velocity { x: 0.0, y: 0.0 },
            paused: false
        });

        // This worker will not move and will never reach its destination
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_STATIC"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: TOTAL_DISTANCE, y: TOTAL_DISTANCE },
            velocity: Velocity { x: 0.0, y: 0.0 },
            paused: false
        });

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates.
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_FRWD_X_INEXACT"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: (TOTAL_DISTANCE - STEP_DISTANCE), y: 0.0 },
            velocity: Velocity { x: (STEP_DISTANCE + 0.1), y: 0.0 },
            paused: false
        });

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates, while moving
        // backwards horizontally.
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_BACK_X_INEXACT"),
            initial_position: Position { x: (TOTAL_DISTANCE - STEP_DISTANCE), y: 0.0 },
            previous_position: Position { x: (TOTAL_DISTANCE - STEP_DISTANCE), y: 0.0 },
            destination: Position { x: 0.0, y: 0.0 },
            velocity: Velocity { x: ((STEP_DISTANCE + 0.1) * -1.0), y: 0.0 },
            paused: false
        });

        // This worker will reach a point near its destination, as opposed to
        // landing at its exact destination coordinates, while moving
        // upwards diagonally.
        test_workers.push(MobilityTestWorker {
            name: String::from("WORKER_FRWD_DIAG_INEXACT"),
            initial_position: Position { x: 0.0, y: 0.0 },
            previous_position: Position { x: 0.0, y: 0.0 },
            destination: Position { x: (TOTAL_DISTANCE - STEP_DISTANCE), y: (TOTAL_DISTANCE - STEP_DISTANCE) },
            velocity: Velocity { x: (STEP_DISTANCE + 0.1), y: (STEP_DISTANCE + 0.1) },
            paused: false
        });

        return test_workers;
    }

    /// Adds the worker to the mobility system
    fn add_to_mobility_system(&self, data : &TestSetup, conn: &PgConnection) {
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


