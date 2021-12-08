extern crate assert_cli;
extern crate chrono;
extern crate mesh_simulator;
extern crate socket2;


use std::collections::HashMap;
use std::env;
use std::iter;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use super::super::*;
use diesel::PgConnection;
use mesh_simulator::logging;
use chrono::Duration;
use mesh_simulator::worker::protocols::ProtocolMessages;
use peroxide::fuga::*;
use slog::*;
use rand::{self, prelude::*};
use pretty_assertions::{assert_eq, assert_ne};
use serde_json;
use std::mem;

use mesh_simulator::backend::*;
// use mesh_simulator::master::Master;
use mesh_simulator::mobility::*;
use mesh_simulator::tests::common::*;
use mesh_simulator::worker::listener::Listener;
use mesh_simulator::worker::radio::{self, *};
use mesh_simulator::worker::worker_config::*;
use mesh_simulator::worker::*;
use mesh_simulator::worker::radio::RadioTypes;

struct TestWorker {
    pub name: String,
    pub short_radio: Arc<dyn Radio>,
    pub short_listener: Box<dyn Listener>,
    pub short_radio_range: f64,
    pub long_radio: Arc<dyn Radio>,
    pub long_listener: Box<dyn Listener>,
    pub long_radio_range: f64,
}

impl TestWorker {
    fn read_message_sync(&self, r_type: RadioTypes, timeout: i64, pauses : i64) -> Option<MessageHeader>{
        // Utc::now().timestamp_nanos() - ts0.timestamp_nanos();
        let start = Utc::now();
        let listener  = match r_type {
            RadioTypes::ShortRange => &self.short_listener,
            RadioTypes::LongRange => &self.long_listener,
        };
        let pause = timeout / pauses;
        let dur = Duration::nanoseconds(pause).to_std().expect("Could not create std duration");
        loop {
            match listener.read_message() {
                Some(m) => {
                    // println!("msg read!");
                    return Some(m)
                },
                None => { 
                    println!("msg not ready to be read");
                    std::thread::sleep(dur);
                },
            }
            if Utc::now().timestamp_nanos() - start.timestamp_nanos() > timeout {
                break;
            }
        }

        return None;
    }

    fn send_message(&self, r_type: RadioTypes, msg : MessageHeader, log_data: ProtocolMessages, logger: &Logger) {
        let radio  = match r_type {
            RadioTypes::ShortRange => &self.short_radio,
            RadioTypes::LongRange => &self.long_radio,
        };
        let tx = radio.broadcast(msg.clone()).unwrap().unwrap();
        // let tx_time = (Utc::now().timestamp_nanos() - t0.timestamp_nanos()) as f64;
        radio::log_tx(
            logger,
            tx,
            &msg.get_msg_id(),
            MessageStatus::SENT,
            &msg.sender,
            &msg.destination,
            log_data,
        );        
    }

    fn add_neighbour(data : &TestSetup, conn: &PgConnection, w1: &Self, name: Option<String>) -> Self {
        let name = name.unwrap_or_else(|| { 
            let n: usize = rand::random();
            format!("worker{}", n)
        });
        let sr_range = w1.short_radio_range;
        let lr_range = w1.long_radio_range;
        let min_distance = sr_range / 10.0;
        let max_distance = sr_range * 0.85;
        let dist: f64 = (rand::random::<f64>() % max_distance) + min_distance;
        let theta: f64 = rand::random::<f64>() % 90.0;
        let y = theta.sin() * dist;
        let x = theta.cos() * dist;
        let pos = Position{x, y};

        return add_worker_to_test(data, conn, Some(name), Some(sr_range), Some(lr_range), Some(pos), None);
    }

    fn add_neighbours(data : &TestSetup, conn: &PgConnection, w1: &Self, num: usize) -> HashMap<String, Self> {
        let mut workers = HashMap::with_capacity(num);
        for _ in 0..num {
            let w = TestWorker::add_neighbour(data, conn, w1, None);
            workers.insert(w.name.clone(), w);
        }

        return workers;
    }
}

fn build_dummy_message(payload_size: usize, ttl: usize) -> (MessageHeader, ProtocolMessages) {
    use mesh_simulator::worker::protocols::flooding;

    let mut rng = thread_rng();
    let mut data: Vec<u8> = iter::repeat(0u8).take(payload_size).collect();
    rng.fill_bytes(&mut data[..]);
    let payload = flooding::Messages::Data(flooding::DataMessage::new(data));
    let log_data = protocols::ProtocolMessages::Flooding(payload.clone());
    let mut msg = MessageHeader::new(
        String::new(),
        String::new(),
        flooding::serialize_message(payload).expect("Could not serialize message"),
    );
    msg.ttl = ttl;
    (msg, log_data)
}

fn add_worker_to_test(
    data : &TestSetup, 
    conn: &PgConnection,
    w_name: Option<String>,
    w_sr_range: Option<f64>,
    w_lr_range: Option<f64>,
    pos: Option<Position>,
    vel: Option<Velocity>,) -> TestWorker {
    
    let mut sr_config = RadioConfig::new();
    let short_radio_range = w_sr_range.unwrap_or(100.0);
    sr_config.range = short_radio_range;

    let mut lr_config = RadioConfig::new();
    let long_radio_range = w_lr_range.unwrap_or(500.0);
    lr_config.range = long_radio_range;

    let work_dir = data.work_dir.clone();
    let worker_name = w_name.unwrap_or(String::from("worker1"));
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
    let random_seed = 1;
    let (r1, l1) = sr_config
        .create_radio(
            OperationMode::Simulated,
            RadioTypes::ShortRange,
            work_dir.clone(),
            worker_name.clone(),
            worker_id.clone(),
            random_seed,
            None,
            data.logger.clone(),
        )
        .expect("Could not create short-range radio");

    let (r2, l2) = lr_config
    .create_radio(
        OperationMode::Simulated,
        RadioTypes::LongRange,
        work_dir,
        worker_name.clone(),
        worker_id.clone(),
        random_seed,
        None,
        data.logger.clone(),
    )
    .expect("Could not create long-range radio");

    // let listener1 = r1.init().unwrap();
    let pos = pos.unwrap_or(Position { x: 0.0, y: 0.0 });
    let vel = vel.unwrap_or(Velocity { x: 0.0, y: 0.0 });
    let _worker_db_id = register_worker(
        &conn,
        worker_name.clone(),
        worker_id,
        pos,
        vel,
        &None,
        Some(r1.get_address().into()),
        Some(r2.get_address().into()),
        &data.logger,
    )
    .expect("Could not register worker");

    return TestWorker{
        name: worker_name,
        short_radio: r1,
        short_listener: l1,
        short_radio_range,
        long_radio: r2,
        long_listener: l2,
        long_radio_range,
    };
}


#[test]
fn test_broadcast_simulated() {
    use mesh_simulator::worker::protocols::flooding;

    //Setup
    let mut data = setup("sim_bcast", false, true);
    // let _res = create_db_objects(&logger).expect("Could not create database objects");
    let env_file = data.db_env_file.take().expect("Failed to unwrap env_file");
    let conn = get_db_connection_by_file(env_file, &data.logger)
        .expect("Could not get DB connection");

    let worker_1 = add_worker_to_test(
        &data,
        &conn,
        Some(String::from("worker1")), 
        Some(100.0), 
        None, 
        Some(Position { x: 0.0, y: 0.0 }), 
        Some(Velocity { x: 0.0, y: 0.0 })
    );

    let worker_2 = TestWorker::add_neighbour(&data, &conn, &worker_1, Some(String::from("worker2")));
    let worker_3 = TestWorker::add_neighbour(&data, &conn, &worker_1, Some(String::from("worker3")));


    //Test checks
    let payload = flooding::Messages::Data(flooding::DataMessage::new(vec![]));
    let log_data = protocols::ProtocolMessages::Flooding(payload.clone());
    let mut msg = MessageHeader::new(
        String::new(),
        String::new(),
        flooding::serialize_message(payload).expect("Could not serialize message"),
    );
    msg.ttl = 1;
    let tx = worker_2.short_radio.broadcast(msg.clone()).unwrap().unwrap();
    radio::log_tx(
        &data.logger,
        tx,
        &msg.get_msg_id(),
        MessageStatus::SENT,
        &msg.sender,
        &msg.destination,
        log_data,
    );
    // let rec_msg1 = worker_1.short_listener.read_message();
    let fifty_ms = 50_000_000;
    let rec_msg1 = worker_1.read_message_sync(RadioTypes::ShortRange, fifty_ms, 50);

    assert!(rec_msg1.is_some());

    let rec_msg3 = worker_3.read_message_sync(RadioTypes::ShortRange, fifty_ms, 50);

    assert!(rec_msg3.is_some());

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&data.work_dir).unwrap();
}


fn test_broadcast_timing_setup_case1(data: &TestSetup, conn: &PgConnection) -> HashMap<String, TestWorker> {
    let mut workers = HashMap::new();
    let worker_1 = add_worker_to_test(
        &data,
        &conn,
        Some(String::from("worker1")), 
        Some(100.0), 
        None, 
        Some(Position { x: 0.0, y: 0.0 }), 
        Some(Velocity { x: 0.0, y: 0.0 })
    );
    
    let w2 = String::from("worker2");
    let worker_2 = TestWorker::add_neighbour(data, conn, &worker_1, Some(w2.clone()));

    workers.insert(worker_1.name.clone(), worker_1);
    workers.insert(w2, worker_2);

    return workers;
}

fn test_broadcast_timing_setup_case2(data: &TestSetup, conn: &PgConnection, workers: HashMap<String, TestWorker>) -> HashMap<String, TestWorker> {
    let w1 = workers.get("worker1").expect("Could not find worker1");
    let new_workers = TestWorker::add_neighbours(data, conn, w1, 8);
    return workers.into_iter().chain(new_workers).collect()
}

fn test_broadcast_timing_setup_case3(data: &TestSetup, conn: &PgConnection, workers: HashMap<String, TestWorker>) -> HashMap<String, TestWorker> { 
    let w1 = workers.get("worker1").expect("Could not find worker1");
    let new_workers = TestWorker::add_neighbours(data, conn, w1, 40);
    workers.into_iter().chain(new_workers).collect()
}

/// This test ensures that the timing of a broadcast operation is not affected by simulation artefacts such as the number of receiving nodes.
#[test]
fn test_broadcast_timing() {
    use mesh_simulator::worker::protocols::flooding;

    //Setup
    let mut data = setup("tx_timing", false, true);
    // let _res = create_db_objects(&logger).expect("Could not create database objects");
    let env_file = data.db_env_file.take().expect("Failed to unwrap env_file");
    let conn = get_db_connection_by_file(env_file, &data.logger)
        .expect("Could not get DB connection");
    println!("Placing test resuls in {}", &data.work_dir);
    //Timeout for reading a message.
    let fifty_ms = 50_000_000;

    ///////////////////////////////////////////////////
    // Case 1: Test tx timing with only one receiver.
    ///////////////////////////////////////////////////
    let workers = test_broadcast_timing_setup_case1(&data, &conn);

    //Tx 10 messages within 10 ms of each other to not clutter the comm channels
    let mut times = Vec::new();
    for _n in 0..10 {
        let payload = flooding::Messages::Data(flooding::DataMessage::new(vec![]));
        let log_data = protocols::ProtocolMessages::Flooding(payload.clone());
        let mut msg = MessageHeader::new(
            String::new(),
            String::new(),
            flooding::serialize_message(payload).expect("Could not serialize message"),
        );
        msg.ttl = 1;
        let t0 = Utc::now();
        let tx = workers["worker1"].short_radio.broadcast(msg.clone()).unwrap().unwrap();
        let tx_time = (Utc::now().timestamp_nanos() - t0.timestamp_nanos()) as f64;
        times.push(tx_time);
        radio::log_tx(
            &data.logger,
            tx,
            &msg.get_msg_id(),
            MessageStatus::SENT,
            &msg.sender,
            &msg.destination,
            log_data,
        );
        let rec_msg1 = workers["worker2"].read_message_sync(RadioTypes::ShortRange, fifty_ms, 50);
        assert!(rec_msg1.is_some());
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let c1_tx_mean = times.mean() / 1000_000f64;
    let c1_tx_std= times.sd() / 1000_000f64;
    let c1_tx_median = times.median() / 1000_000f64;
    debug!(&data.logger, "[Case1] Tx statistics:";"mean"=>c1_tx_mean, "sd"=>c1_tx_std, "tx_median"=>c1_tx_median);
    println!("\n[Case1] Tx statistics: mean={:.5}ms, sd={:.5}ms, tx_median={:.5}ms", c1_tx_mean, c1_tx_std, c1_tx_median);
    

    ///////////////////////////////////////////////////
    // Case 2: Test tx timing with 10 receivers.
    ///////////////////////////////////////////////////
    let workers = test_broadcast_timing_setup_case2(&data, &conn, workers);
    
    //Tx 10 messages within 10 ms of each other to not clutter the comm channels
    times.clear();
    let w1 = workers.get("worker1").expect("Could not find worker1");
    for _n in 0..10 {
        let payload = flooding::Messages::Data(flooding::DataMessage::new(vec![]));
        let log_data = protocols::ProtocolMessages::Flooding(payload.clone());
        let mut msg = MessageHeader::new(
            String::new(),
            String::new(),
            flooding::serialize_message(payload).expect("Could not serialize message"),
        );
        msg.ttl = 1;
        let t0 = Utc::now();
        let tx = w1.short_radio.broadcast(msg.clone()).unwrap().unwrap();
        let tx_time = (Utc::now().timestamp_nanos() - t0.timestamp_nanos()) as f64;
        times.push(tx_time);
        radio::log_tx(
            &data.logger,
            tx,
            &msg.get_msg_id(),
            MessageStatus::SENT,
            &msg.sender,
            &msg.destination,
            log_data,
        );

        for (k,v) in workers.iter() {
            if k == "worker1" {
                continue;
            }
            let rec_msg = v.read_message_sync(RadioTypes::ShortRange, fifty_ms, 50);
            assert!(rec_msg.is_some());
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let c2_tx_mean = times.mean() / 1000_000f64;
    let c2_tx_std= times.sd() / 1000_000f64;
    let c2_tx_median = times.median() / 1000_000f64;
    debug!(&data.logger, "[Case2] Tx statistics:";"mean"=>c2_tx_mean, "sd"=>c2_tx_std, "tx_median"=>c2_tx_median);
    println!("[Case2] Tx statistics: mean={:.5}ms, sd={:.5}ms, tx_median={:.5}ms", c2_tx_mean, c2_tx_std, c2_tx_median);

    ///////////////////////////////////////////////////
    // Case 3: Test tx timing with 50 receivers.
    ///////////////////////////////////////////////////
    let workers = test_broadcast_timing_setup_case3(&data, &conn, workers);
    
    //Tx 10 messages within 10 ms of each other to not clutter the comm channels
    times.clear();
    let w1 = workers.get("worker1").expect("Could not find worker1");
    for _n in 0..10 {
        let payload = flooding::Messages::Data(flooding::DataMessage::new(vec![]));
        let log_data = protocols::ProtocolMessages::Flooding(payload.clone());
        let mut msg = MessageHeader::new(
            String::new(),
            String::new(),
            flooding::serialize_message(payload).expect("Could not serialize message"),
        );
        msg.ttl = 1;
        let t0 = Utc::now();
        let tx = w1.short_radio.broadcast(msg.clone()).unwrap().unwrap();
        let tx_time = (Utc::now().timestamp_nanos() - t0.timestamp_nanos()) as f64;
        times.push(tx_time);
        radio::log_tx(
            &data.logger,
            tx,
            &msg.get_msg_id(),
            MessageStatus::SENT,
            &msg.sender,
            &msg.destination,
            log_data,
        );

        for (k,v) in workers.iter() {
            if k == "worker1" {
                continue;
            }
            let rec_msg = v.read_message_sync(RadioTypes::ShortRange, fifty_ms, 50);
            assert!(rec_msg.is_some());
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let c3_tx_mean = times.mean() / 1000_000f64;
    let c3_tx_std= times.sd() / 1000_000f64;
    let c3_tx_median = times.median() / 1000_000f64;
    debug!(&data.logger, "[Case3] Tx statistics:";"mean"=>c3_tx_mean, "sd"=>c3_tx_std, "tx_median"=>c3_tx_median);
    println!("[Case3] Tx statistics: mean={:.5}ms, sd={:.5}ms, tx_median={:.5}ms", c3_tx_mean, c3_tx_std, c3_tx_median);

    let c1_c2_diff = (c2_tx_mean - c1_tx_mean).abs();
    let c1_c3_diff = (c3_tx_mean - c1_tx_mean).abs();
    let c2_c3_diff = (c3_tx_mean - c2_tx_mean).abs();
    let max_mean = {
        let m = if c3_tx_mean > c2_tx_mean {
            c3_tx_mean
        } else {
            c2_tx_mean
        };
        if c1_tx_mean > m {
            c1_tx_mean
        } else {
            m
        }
    };
    let threshold = max_mean * 0.15;
    println!("Threshold: {}, c1_c2_diff={}, c1_c3_diff={}, c2_c3_diff={}", threshold, c1_c2_diff, c1_c3_diff, c2_c3_diff);
    assert!(c1_c2_diff <= threshold);
    assert!(c1_c3_diff <= threshold);
    assert!(c2_c3_diff <= threshold);

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&data.work_dir).unwrap();
}

fn broadcast_delay_setup(data: &TestSetup, conn: &PgConnection) -> HashMap<String, TestWorker> {
    let w1 = add_worker_to_test(
        &data,
        &conn,
        Some(String::from("worker1")), 
        Some(100.0), 
        None, 
        Some(Position { x: 0.0, y: 0.0 }), 
        Some(Velocity { x: 0.0, y: 0.0 })
    );

    let mut workers = TestWorker::add_neighbours(data, conn, &w1, 29);
    workers.insert(w1.name.clone(), w1);

    return workers;
}

/// This test ensures the total TX time of a broadcast is capped and  that all nodes receive a msg simultaneously.
#[test]
fn test_broadcast_delay() {
    //Setup
    let mut data = setup("tx_delay", false, true);
    // let _res = create_db_objects(&logger).expect("Could not create database objects");
    let env_file = data.db_env_file.take().expect("Failed to unwrap env_file");
    let conn = get_db_connection_by_file(env_file, &data.logger)
        .expect("Could not get DB connection");
    println!("Placing test resuls in {}", &data.work_dir);
    //Timeout for reading a message.
    let fifty_ms = 50_000_000;
    let workers = broadcast_delay_setup(&data, &conn);

    let w1 = workers.get("worker1").expect("Could not find worker1");
    for _n in 0..10 {
        //Build the message to send
        let (msg, log_data) = build_dummy_message(0, 1);
        //Broadcast the message
        w1.send_message(RadioTypes::ShortRange, msg, log_data, &data.logger);

        //Make sure each neighbour receives it
        for (k,v) in workers.iter() {
            if k == "worker1" {
                continue;
            }
            let rec_msg = v.read_message_sync(RadioTypes::ShortRange, fifty_ms, 1000);
            assert!(rec_msg.is_some());
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    //Get all outgoing message records
    let log_file = Path::new(&data.work_dir)
        .join("tx_delay.log");
    let last_tranmission_msgs: HashMap<String, DateTime<Utc>> =  get_last_transmission_records(&log_file)
            .expect("Could not read last_transmission messages")
            .drain(0..)
            .map(|x| { 
                let msg_id = x["msg_id"].as_str().expect("Log record did not have a msg_id").to_string();
                let ts = x["ts"]
                    .as_str()
                    .expect("Log record did not have a msg_id")
                    .parse::<DateTime<Utc>>()
                    .expect("Could not parse string into Datetime");

                (msg_id, ts)
            })
            .collect();
    println!("{} outgoing messages", last_tranmission_msgs.len());

    //Get all incoming message records
    let incoming_msgs: Vec<(String, IncomingTxLog)> = get_incoming_transmission_records(&log_file)
        .expect("Could not read incoming packets")
        .drain(0..)
        .map(|x| (x.msg_id.clone(), x))
        .collect();
    println!("{} incoming messages", incoming_msgs.len());

    //For every transmitted message, find each reception by its neighbours and measure the delay between tx and rx.
    let mut delays = vec![];
    let mut times_per_message = HashMap::new();
    for (tx_id, tx_log) in &last_tranmission_msgs {
        times_per_message.insert(tx_id.clone(), vec![]);
        for (rx_id, rx_log) in &incoming_msgs {
            if tx_id == rx_id {
                let t = times_per_message.get_mut(rx_id).expect("Message not present");
                let tx_ts = tx_log.timestamp_nanos();
                t.push(tx_ts);
                delays.push(rx_log.duration as f64);
            }
        }
    }
    
    //Assert that no message took longer than the upper bound
    info!(&data.logger, "Rx delay stats"; "mean"=>delays.mean(), "median"=>delays.median(), "sd"=>delays.sd());
    println!("***Rx delay stats***\nmean:\t{}\nmedian:\t{}\nsd:\t{}", delays.mean(), delays.median(), delays.sd());
    assert_eq!(
        0,
        delays
        .iter()
        .filter(|&&x| x >= (radio::TX_DURATION + radio::TX_VARIABILITY) as f64)
        .count()
    );

    // //Assert that no message was shorter than the lower bound
    // assert_eq!(
    //     0,
    //     delays
    //     .iter()
    //     .filter(|&&x| x < (radio::TX_DURATION - radio::TX_VARIABILITY) as f64)
    //     .count()
    // );

    //Assert that the difference in reception times between each receiver of the same message is minimal.
    for (_msd_id, mut rx_times) in times_per_message {
        //Assert that difference between all subsequent pairs of elements is less than the expected TX_Variability
        let res = rx_times
            .windows(2)
            .all(|w| {
                let delta = w[0] - w[1];
                delta.abs() < radio::TX_VARIABILITY
            });
        assert!(res);

        //Assert that the different between the first and last rx event is less than the expected TX_Variability
        rx_times.sort_by(|&a, &b| a.partial_cmp(&b).unwrap());
        let diff = rx_times.last().unwrap() - rx_times[0];
        assert!( diff.abs() < radio::TX_VARIABILITY );
    }
 
    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&data.work_dir).unwrap();
}

fn tx_bandwidth_setup(data: &TestSetup, conn: &PgConnection) -> HashMap<String, TestWorker> {
    let w1 = add_worker_to_test(
        &data,
        &conn,
        Some(String::from("worker1")), 
        Some(100.0), 
        None, 
        Some(Position { x: 0.0, y: 0.0 }), 
        Some(Velocity { x: 0.0, y: 0.0 })
    );

    // let mut workers = TestWorker::add_neighbours(data, conn, &w1, 10);
    let mut workers = HashMap::new();
    workers.insert(w1.name.clone(), w1);

    return workers;
}

//#[ignore]
#[test]
fn test_tx_bandwidth() {
    //Setup
    let test_name = "tx_bandwidth";
    let mut data = setup(test_name, false, true);
    // let _res = create_db_objects(&logger).expect("Could not create database objects");
    let env_file = data.db_env_file.take().expect("Failed to unwrap env_file");
    let conn = get_db_connection_by_file(env_file, &data.logger)
        .expect("Could not get DB connection");
    println!("Placing test resuls in {}", &data.work_dir);
    //Timeout for reading a message.
    let fifty_ms = 50_000_000;
    let pauses = 1000;
    let MAX_NODES: usize = 75;
    let PACKETS_PER_SAMPLE: usize = 25;
    let MAX_PACKET_SIZE: usize = 50000;
    let PACKET_SIZE_STEP: usize = 500;
    let mut workers = tx_bandwidth_setup(&data, &conn);

    for _ in 0..MAX_NODES {
        let w = {
            let w1 = workers.get("worker1").expect("Could not find worker1");
            TestWorker::add_neighbour(&data, &conn, &w1, None)
        };
        workers.insert(w.name.clone(), w);

        for packet_size in (0..MAX_PACKET_SIZE+PACKET_SIZE_STEP).step_by(PACKET_SIZE_STEP) {
            let num_neighbours = workers.len();
            for _ in 0..PACKETS_PER_SAMPLE {
                let w1 = workers.get("worker1").expect("Could not find worker1");
                let (msg, log_data) = build_dummy_message(packet_size, 1);
                //Broadcast the message
                w1.send_message(RadioTypes::ShortRange, msg, log_data, &data.logger);
        
                //Make sure each neighbour receives it
                for (k,v) in workers.iter() {
                    // println!("k={}", k);
                    if k == "worker1" {
                        continue;
                    }
                    let rec_msg = v.read_message_sync(RadioTypes::ShortRange, fifty_ms, pauses);
                    assert!(rec_msg.is_some());
                }
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
            println!("Nodes:{}, PacketSize:{} - Done!", num_neighbours-1, packet_size);
        }
    }

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    // let _res = std::fs::remove_dir_all(&data.work_dir).unwrap();
}


#[test]
fn test_mac_layer_basic() {
    let test_name = String::from("mac_layer_basic");
    let data = setup(&test_name, false, false);

    println!(
        "Running command: {} -t {} -w {} -d {}",
        &data.master, &data.test_file, &data.worker, &data.work_dir
    );

    //Assert the test finished succesfully
    assert_cli::Assert::command(&[&data.master])
        .with_args(&[
            "-t",
            &data.test_file,
            "-w",
            &data.worker,
            "-d",
            &data.work_dir,
        ])
        .succeeds()
        .unwrap();

    //Check the test ended with the correct number of processes.
    let master_log_file = format!(
        "{}{}{}{}{}",
        &data.work_dir,
        std::path::MAIN_SEPARATOR,
        LOG_DIR_NAME,
        std::path::MAIN_SEPARATOR,
        DEFAULT_MASTER_LOG
    );
    let master_log_records = logging::get_log_records_from_file(&master_log_file).unwrap();
    let master_node_num = logging::find_record_by_msg(
        "End_Test action: Finished. 25 processes terminated.",
        &master_log_records,
    );
    assert!(master_node_num.is_some());

    let node4_log_file = &format!("{}/log/node4.log", &data.work_dir);
    let incoming_messages = logging::get_received_message_records(node4_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node5_log_file = &format!("{}/log/node5.log", &data.work_dir);
    let incoming_messages = logging::get_received_message_records(node5_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node19_log_file = &format!("{}/log/node19.log", &data.work_dir);
    let incoming_messages = logging::get_received_message_records(node19_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node20_log_file = &format!("{}/log/node20.log", &data.work_dir);
    let incoming_messages = logging::get_received_message_records(node20_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node22_log_file = &format!("{}/log/node22.log", &data.work_dir);
    let incoming_messages = logging::get_received_message_records(node22_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node24_log_file = &format!("{}/log/node24.log", &data.work_dir);
    let incoming_messages = logging::get_received_message_records(node24_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    let node25_log_file = &format!("{}/log/node25.log", &data.work_dir);
    let incoming_messages = logging::get_received_message_records(node25_log_file).unwrap();
    let msg_received = incoming_messages
        .iter()
        .filter(|&m| m.msg_type == "DATA" && m.status == "ACCEPTED")
        .count();
    assert_eq!(msg_received, 1);

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    teardown(data, true);
}

#[test]
fn test_broadcast_device() -> TestResult {
    use mesh_simulator::worker::protocols::flooding;

    //Setup
    let host = env::var("MESHSIM_HOST").unwrap_or_else(|_| String::from(""));
    //This test should ONLY run on my lab development machine due to required configuration of device_mode.
    if !host.eq("kaer-morhen") {
        println!("This test should only run in the kaer-morhen host");
        return Ok(());
    }

    //Acquire the lock for the NIC since other tests also require it and they conflict with each other.
    let _nic = WIRELESS_NIC.lock()?;

    //Get general test settings
    let test_name = String::from("dev_bcast");
    let data = setup(&test_name, false, true);

    //init_logger(&test_path, "device_bcast_test");
    println!("Test results placed in {}", &data.work_dir);

    //Worker1
    let sr_config1 = RadioConfig::new();
    let work_dir = data.work_dir.clone();
    let worker_name = String::from("worker1");
    let worker_id = String::from("416d77337e24399dc7a5aa058039f72a"); //arbitrary
    let random_seed = 1;
    let (r1, l1) = sr_config1
        .create_radio(
            OperationMode::Device,
            RadioTypes::ShortRange,
            work_dir,
            worker_name,
            worker_id,
            random_seed,
            None,
            data.logger.clone(),
        )
        .expect("Could not create radio for worker1");
    // let listener1 = r1.init().unwrap();

    //Test checks
    let msg = flooding::Messages::Data(flooding::DataMessage::new(vec![]));
    let log_data = protocols::ProtocolMessages::Flooding(msg.clone());
    let bcast_msg = MessageHeader::new(
        String::new(),
        String::new(),
        flooding::serialize_message(msg).expect("Could not serialize message"),
    );
    let tx = r1.broadcast(bcast_msg.clone()).unwrap().unwrap();
    radio::log_tx(
        &&data.logger,
        tx,
        &bcast_msg.get_msg_id(),
        MessageStatus::SENT,
        &bcast_msg.sender,
        &bcast_msg.destination,
        log_data,
    );
    //We only test that the broadcast was received by the broadcaster, since we can only deploy 1 device_mode worker
    //per machine.
    let rec_msg1 = l1.read_message();
    assert!(rec_msg1.is_some());
    let rec_msg1 = rec_msg1.unwrap();

    assert_eq!(bcast_msg.get_payload(), rec_msg1.get_payload());

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    teardown(data, false);

    Ok(())
}

#[test]
fn test_last_transmission() {
    use mesh_simulator::worker::protocols::flooding;

    let test_name = "last_transmission";
    let data = setup(test_name, false, true);
    let conn = get_db_connection_by_file(data.db_env_file.unwrap(), &data.logger)
        .expect("Could not connect");

    let worker_name = String::from("node1");
    let worker_id = String::from("SOME_UNIQUE_ID");
    let random_seed = 12345;
    let config = RadioConfig::new();
    let (tx, _rx) = config
        .create_radio(
            OperationMode::Simulated,
            RadioTypes::ShortRange,
            data.work_dir.clone(),
            worker_name.clone(),
            worker_id.clone(),
            random_seed,
            // Some(Arc::clone(&rng)),
            None,
            data.logger.clone(),
        )
        .expect("Could not create radio-channels");
    let radio_address = tx.get_address();
    let pos = Position { x: 0.0, y: 0.0 };
    let vel = Velocity { x: 0.0, y: 0.0 };
    let dest = None;
    let _db_id = register_worker(
        &conn,
        worker_name,
        worker_id,
        pos,
        vel,
        &dest,
        Some(radio_address.to_string()),
        None,
        &data.logger,
    )
    .expect("Could not register worker");

    //Time before
    let ts1 = Utc::now();

    let msg = flooding::Messages::Data(flooding::DataMessage::new(vec![]));
    let log_data = protocols::ProtocolMessages::Flooding(msg.clone());
    let hdr = MessageHeader::new(
        String::new(),
        String::new(),
        flooding::serialize_message(msg).expect("Could not serialize message"),
    );
    let tx_md = tx
        .broadcast(hdr.clone())
        .expect("Could not broadcast message")
        .unwrap();
    radio::log_tx(
        &data.logger,
        tx_md,
        &hdr.get_msg_id(),
        MessageStatus::SENT,
        &hdr.sender,
        &hdr.destination,
        log_data,
    );
    //Broadcast time
    let bc_ts = tx.last_transmission().load(Ordering::SeqCst);

    //Now
    let ts2 = Utc::now();

    assert!(ts1.timestamp_nanos() < bc_ts);
    assert!(bc_ts < ts2.timestamp_nanos());

    //Teardown
    //If test checks fail, this section won't be reached and not cleaned up for investigation.
    let _res = std::fs::remove_dir_all(&data.work_dir).unwrap();

}
