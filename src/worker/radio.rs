//! This module defines the abstraction and functionality for what a Radio is in MeshSim

use crate::backend::*;
use crate::worker::listener::*;
use crate::worker::*;
use crate::{MeshSimError, MeshSimErrorKind};
use chrono::Utc;
use diesel::pg::PgConnection;
use pnet_datalink as datalink;


use slog::{Logger, KV};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::mem;

#[cfg(target_os = "linux")]
use self::sx1276::socket::{Link, LoRa};
#[cfg(target_os = "linux")]
use linux_embedded_hal as hal;
#[cfg(target_os = "linux")]
use sx1276;

const SHORT_RANGE_DIR: &str = "SHORT";
const LONG_RANGE_DIR: &str = "LONG";
// const DELAY_PER_NODE: i64 = 50; //µs
pub const DEFAULT_TRANSMISSION_MAX_RETRY: usize = 8;
const TRANSMISSION_EXP_CAP: u32 = 10;
const TRANSMITTER_REGISTER_MAX_RETRY: usize = 10;
pub const DEFAULT_TRANSMISSION_WAIT_BASE: u64 = 16; //µs
pub const TX_DURATION: i64 = 2_805_900;
pub const TX_VARIABILITY: i64 = 581_816;
///Maximum size the payload of a UDP packet can have.
pub const MAX_UDP_PAYLOAD_SIZE: usize = 65507; //65,507 bytes (65,535 − 8 byte UDP header − 20 byte IP header)

lazy_static! {
    ///Address used for multicast group
    pub static ref SERVICE_ADDRESS: Ipv6Addr = Ipv6Addr::new(0xFF02, 0, 0, 0, 0, 0, 0, 0x0123);
}

// Helper functions for the whole module
fn calculate_signal_loss(
    tx_loss_at_ref_dist: f64,
    power_loss_coefficient: f64,
    distance: f64,
    reference_dist: f64,
    floor_penetration_loss_factor: f64) -> f64 {
        
    assert!(reference_dist > 0f64);
    assert!(power_loss_coefficient > 0f64);
    
    if distance <= 0f64 {
        return 0f64;
    }

    //Adjust distance if necessary to avoid getting a negative lost
    let distance = if distance < reference_dist {
        reference_dist
    } else {
        distance
    };

    let loss = tx_loss_at_ref_dist
    + power_loss_coefficient*(distance/reference_dist).log(10.0)
    + floor_penetration_loss_factor;

    return loss;
}

/// Metadata referring to a radio transmission
#[derive(Default)]
pub struct TxMetadata {
    /// Radio type (short/long)
    pub radio_type: String,
    /// Id of the thread that made the transmission
    pub thread_id: String,
    /// Duration (in nanoseconds) the packet spent in the out_queue
    pub out_queued_duration: i64,
    /// Duration (in nanoseconds) the packet spent waiting on medium contention.
    pub contention_duration: i64,
    /// Duration (in nanoseconds) the radio took to transmit the packet to all recipients
    pub tx_duration: i64,
    /// Duration (in nanoseconds) the radio took in the whole broadcast process.
    /// Should be roughly equal to contention_duration + tx_duration
    pub broadcast_duration: i64,
    /// Size in bytes of the transmitted data
    pub size: usize,
    /// Number of peers in range when the tx happened
    pub peers_in_range: usize,
}

///Types of radio supported by the system. Used by Protocols that need to
/// request an operation from the worker on a given radio.
#[derive(Debug, Clone, Copy)]
pub enum RadioTypes {
    ///Represents the longer-range radio amongst the available ones.
    LongRange,
    ///Represents the short-range, data radio.
    ShortRange,
}

impl Into<String> for RadioTypes {
    fn into(self) -> String {
        match self {
            RadioTypes::LongRange => String::from(LONG_RANGE_DIR),
            RadioTypes::ShortRange => String::from(SHORT_RANGE_DIR),
        }
    }
}

impl FromStr for RadioTypes {
    type Err = MeshSimError;

    fn from_str(s: &str) -> Result<RadioTypes, MeshSimError> {
        match s.to_lowercase().as_str() {
            LONG_RANGE_DIR => Ok(RadioTypes::LongRange),
            SHORT_RANGE_DIR => Ok(RadioTypes::ShortRange),
            &_ => {
                let err_msg = format!("Unknown radio type {}", s);
                let error = MeshSimError {
                    kind: MeshSimErrorKind::Configuration(err_msg),
                    cause: None,
                };
                Err(error)
            }
        }
    }
}
/// Trait for all types of radios.
pub trait Radio: std::fmt::Debug + Send + Sync {
    ///Gets the current address at which the radio is listening.
    fn get_address(&self) -> &str;
    ///Used to broadcast a message using the radio
    fn broadcast(&self, hdr: MessageHeader) -> Result<Option<TxMetadata>, MeshSimError>;
    ///Get the time of the last transmission made by this readio
    fn last_transmission(&self) -> Arc<AtomicI64>;
    /// Get the max signal loss possible for this radio based on its physical configuration
    fn get_max_loss(&self) -> f64;
}

/// Represents a radio used by the worker to send a message to the network.
#[derive(Debug)]
pub struct SimulatedRadio {
    /// Timeout for every read operation of the radio.
    pub timeout: u64,
    ///The work dir of the worker that owns this radio.
    pub work_dir: String,
    /// The maximum signal loss for this radio based on its physical properties
    pub max_loss: f64,
    ///Address that this radio listens on
    address: String,
    ///Name of the node that owns this radio
    worker_name: String,
    ///The unique id of the worker that uses this radio. The id is shared across radios belonging to the same worker.
    id: String,
    ///Short or long range. What's the range-role of this radio.
    r_type: RadioTypes,
    ///Range of this worker
    range: f64,
    ///Max number of retries for the mac layer CDMA/CA algorithm
    mac_layer_retries: usize,
    ///The base wait-number in milliseconds for each time the mac layer has a collition
    mac_layer_base_wait: u64,
    ///Random number generator used for all RNG operations.
    rng: Arc<Mutex<StdRng>>,
    /// Logger for this Radio to use.
    logger: Logger,
    /// Use for syncronising thread operations on the DB
    transmitting: Arc<Mutex<()>>,
    /// The last time this radio made a transmission
    last_transmission: Arc<AtomicI64>,
    // Transmission power used to calculate the signal strength.
    transmission_power: u8,
    //Transmission loss at reference distance.
    tx_loss_at_ref_dist: f64,
    // Power loss coefficient used to calculate the signal strength.
    power_loss_coefficient: f64,
    // Reference distance used to calculate the signal strength.
    reference_distance: f64,
    // Floor penetration factor used to calculate the signal strength.
    floor_penetration_loss_factor: f64,
    // Frequency in mhz used to calculate the signal strength.
    frequency_in_mhz: f64,

}

impl Radio for SimulatedRadio {
    fn get_address(&self) -> &str {
        &self.address
    }

    fn last_transmission(&self) -> Arc<AtomicI64> {
        // self.last_transmission.load(Ordering::SeqCst)
        Arc::clone(&self.last_transmission)
    }

    fn get_max_loss(&self) -> f64 {
        self.max_loss
    }

    fn broadcast(&self, mut hdr: MessageHeader) -> Result<Option<TxMetadata>, MeshSimError> {
        //Perf measurement. How long was the message in the out queue?
        let start_ts = Utc::now();
        debug!(&self.logger, "out_queued_start: {}", hdr.delay);
        let perf_out_queued_duration = start_ts.timestamp_nanos() - hdr.delay;
        let radio_range: String = self.r_type.into();
        let thread_id = format!("{:?}", thread::current().id());

        if hdr.ttl <= 0 {
            //TTL expired for this packet
            info!(
                &self.logger,
                "Not transmitting";
                "thread"=>&thread_id,
                "radio"=>&radio_range,
                "destination" => &hdr.destination,
                "source" => &hdr.sender,
                "hops" => hdr.hops,
                "reason"=>"TTL depleted",
                "status" => MessageStatus::DROPPED,
                "msg_id" => &hdr.msg_id,
            );
            return Ok(None)
        }

        // let env_file = format!("{}{}.env", &self.work_dir, std::path::MAIN_SEPARATOR);
        let conn = get_db_connection(&self.logger)?;


        // let msg_id = hdr.get_msg_id().to_string();

        //Register this node as an active transmitter if the medium is free
        let guard = self.register_transmitter(&conn, &hdr.msg_id);

        //Perf measurement. How long the process stuck in medium-contention?
        //Measured here since it is logged even if the transmission is aborted for profiling purposes.
        let perf_contention_duration = Utc::now().timestamp_nanos() - start_ts.timestamp_nanos();

        //Was this node able to acquire the medium?
        let guard = match guard {
            Ok(g) => {
                /* All good */
                g
            }
            Err(e) => {
                if let MeshSimErrorKind::NetworkContention(m) = e.kind {
                    // let cause = e.cause.unwrap_or(Box::new(String::from()));
                    let cause = e
                        .cause
                        .expect("ERROR: Cause not set for NetworkContention event");
                    let cause_str = format!("{}", cause);
                    info!(
                        &self.logger,
                        "{}", m;
                        "status" => MessageStatus::DROPPED,
                        "msg_id" => hdr.get_msg_id(),
                        "thread"=>&thread_id,
                        "radio"=>&radio_range,
                        "out_queued_duration"=>perf_out_queued_duration,
                        "contention_duration"=>perf_contention_duration,
                        "reason"=>cause_str,
                    );
                    return Ok(None);
                } else {
                    return Err(e);
                }
            }
        };


        //Increase the hop count of the message
        hdr.hops += 1;

        //Get the list of peers in radio-range
        let peers = get_workers_in_range(&conn, &self.worker_name, self.range, &self.logger)?;
        let num_peers = peers.len();
        info!(
            self.logger,
            "Starting transmission";
            "msg_id"=>&hdr.get_msg_id(),
            "thread"=>&thread_id,
            //"peers_in_range"=>num_peers,
            "radio"=>&radio_range,
        );

        //Adding duration here just for the purposes of getting the size of the packet
        hdr.delay = TX_DURATION;
        let packet_size = hdr.to_vec()
            .map(|v| mem::size_of_val(&*v))?;

        debug!(self.logger, "Packet size: {}", packet_size);

        let socket = new_socket()?;
        // let mut acc_delay: i64 = 0;
        // let max_delay = std::cmp::min(MAX_DELAY_PEERS, peers.len() as i64) * self.max_delay_per_node;
        // let delay_per_node = max_delay / std::cmp::max(1, peers.len()) as i64;
        //Start measuring how long it takes to perform the actual transmission
        let perf_start_tx = Utc::now();
        for p in peers.into_iter() {
            let mut msg = hdr.clone();

            msg.signal_loss = self.tx_loss_at_ref_dist
                + self.power_loss_coefficient*(p.distance/self.reference_distance).log(10.0)
                + self.floor_penetration_loss_factor;
                
            debug!(self.logger,
                "Worker {} found peer {} with distance: {} and signal_loss: {}",
                &self.worker_name, p.worker_name, p.distance, msg.signal_loss);

            //TODO: review this calculation. Should I be substracting TX_VARIABILITY?
            msg.delay = std::cmp::max(
                TX_DURATION - 
                    (Utc::now().timestamp_nanos() - perf_start_tx.timestamp_nanos()) -
                    TX_VARIABILITY,
                0
            );

            let data = msg.to_vec()?;
            let selected_address = match self.r_type {
                RadioTypes::ShortRange => p.short_range_address.clone(),
                RadioTypes::LongRange => p.long_range_address.clone(),
            };

            if let Some(addr) = selected_address {
                let remote_addr: SocketAddr = match addr.parse::<SocketAddr>() {
                    Ok(sock_addr) => sock_addr,
                    Err(e) => {
                        warn!(
                            &self.logger,
                            "Unable to parse {} into SocketAddr: {}", &addr, e
                        );
                        continue;
                    }
                };

                match socket.send_to(&data, &socket2::SockAddr::from(remote_addr)) {
                    Ok(_) => {
                        /* All good */
                        // debug!(&self.logger, "Message sent to {:?}", &remote_addr);
                    }
                    Err(e) => {
                        warn!(
                            &self.logger,
                            "Failed to send data to {}. Error: {}", &remote_addr, e
                        );
                        continue;
                    }
                };
            } else {
                error!(
                    self.logger,
                    "No known address for peer {} in range {:?}", p.worker_name, self.r_type
                );
            }
        }

        
        //Time it took for the actual transmission to execute (e.g. iteratiing through the peers and sending the messages)
        let perf_end_tx = Utc::now().timestamp_nanos();
        let perf_tx_duration = perf_end_tx - perf_start_tx.timestamp_nanos();
        
        //Update last transmission time
        self.last_transmission
            .store(perf_end_tx, Ordering::SeqCst);
        info!(
            &self.logger,
            "last_transmission:{}",
            self.last_transmission.load(Ordering::SeqCst);
            "msg_id"=>&hdr.get_msg_id()
        );

        self.deregister_transmitter(&conn, guard)?;
        
        //Time it took for the whole function to execute
        let broadcast_duration = Utc::now().timestamp_nanos() - start_ts.timestamp_nanos();

        let tx = TxMetadata {
            radio_type: radio_range,
            thread_id,
            out_queued_duration: perf_out_queued_duration,
            contention_duration: perf_contention_duration,
            tx_duration: perf_tx_duration,
            broadcast_duration,
            size: packet_size,
            peers_in_range: num_peers,
        };

        Ok(Some(tx))
    }
}

impl SimulatedRadio {
    /// Constructor for new Radios
    pub fn new(
        timeout: u64,
        work_dir: String,
        id: String,
        worker_name: String,
        r_type: RadioTypes,
        range: f64,
        mac_layer_retries: usize,
        mac_layer_base_wait: u64,
        transmission_power: u8,
        power_loss_coefficient: f64,
        reference_distance: f64,
        floor_penetration_loss_factor: f64,
        frequency_in_mhz: f64,
        rng: Arc<Mutex<StdRng>>,
        logger: Logger,
    ) -> Result<(SimulatedRadio, Box<dyn Listener>), MeshSimError> {
        // let address = SimulatedRadio::format_address(&work_dir, &id, r_type);
        let listener = SimulatedRadio::init(timeout, &rng, r_type, &logger)?;
        let listen_addres = listener.get_address();
        let tx_loss_at_ref_dist = 20.0f64*(frequency_in_mhz).log(10.0) - 28.0f64;
        let max_loss = calculate_signal_loss(
            tx_loss_at_ref_dist,
            power_loss_coefficient,
            range,
            reference_distance,
            floor_penetration_loss_factor
        );
        let sr = SimulatedRadio {
            timeout,
            work_dir,
            max_loss,
            id,
            worker_name,
            address: listen_addres,
            range,
            r_type,
            mac_layer_retries,
            mac_layer_base_wait,
            rng,
            logger,
            transmitting: Arc::new(Mutex::new(())),
            last_transmission: Default::default(),
            transmission_power,
            tx_loss_at_ref_dist,
            power_loss_coefficient,
            reference_distance,
            floor_penetration_loss_factor,
            frequency_in_mhz,
        };
        Ok((sr, listener))
    }

    fn init(
        timeout: u64,
        rng: &Arc<Mutex<StdRng>>,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<Box<dyn Listener>, MeshSimError> {
        let sock = new_socket()?;
        sock.set_only_v6(true).map_err(|e| {
            let err_msg = String::from("Failed to configure socket");
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

        // Listen on port 0 to be auto-assigned an address
        let base_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)), 0);

        sock.bind(&SockAddr::from(base_addr)).map_err(|e| {
            let err_msg = format!("Could not bind socket to address {}", &base_addr);
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        let listener =
            SimulatedListener::new(sock, timeout, Arc::clone(rng), r_type, logger.clone());
        // let radio_type: String = r_type.into();
        // print!("{}-{};", radio_type, &listener.get_address());

        Ok(Box::new(listener))
    }

    fn register_transmitter(&self, conn: &PgConnection, msg_id: &str) -> Result<MutexGuard<()>, MeshSimError> {
        // let tx = start_tx(&mut conn)?;
        let mut i = 0;
        // let wait_base = self.get_wait_base();
        let thread_id = format!("{:?}", thread::current().id());
        let radio_range: String = self.r_type.into();

        info!(
            &self.logger,
            "Attempting to acquire the medium";
            "thread"=>&thread_id,
            "radio"=>&radio_range,
            "msg_id"=>msg_id,
        );

        //Only one thread can transmit simultaneously, as in real life, the NIC can't broadcast more 
        //than one signal concurrently.
        let guard = self
            .transmitting
            .lock()
            .expect("Could not acquire transmitter lock");

        //The config text mac_base_Retry, so it should retry up to that number (the initial attempt is not a retry.)
        while i <= self.mac_layer_retries {
            let access_start = Utc::now();
            match register_active_transmitter_if_free(
                conn,
                &self.worker_name,
                self.r_type,
                self.range,
                &self.logger,
            ) {
                Ok(rows) => {
                    if rows > 0 {
                        let medium_access_time = Utc::now().timestamp_nanos() - access_start.timestamp_nanos();
                        info!(
                            &self.logger,
                            "Registered as an active transmitter";
                            "thread"=>&thread_id,
                            "medium_access_time"=> medium_access_time,
                            "msg_id"=>msg_id,
                            // "name"=>&self.worker_name,
                            "radio"=>&radio_range,
                        );

                        return Ok(guard);
                    }
                }
                Err(e) => {
                    warn!(self.logger, "{}", &e; "Thread"=> &thread_id);
                    if let Some(cause) = e.cause {
                        warn!(self.logger, "Cause: {}", &cause);
                    }
                }
            }

            //Wait and retry.
            let medium_access_time = Utc::now().timestamp_nanos() - access_start.timestamp_nanos();
            i += 1;
            let sleep_time = self.get_wait_time(i as u32);
            // let st = format!("{:?}", &sleep_time);
            info!(
                &self.logger,
                "Medium is busy";
                "thread"=>&thread_id,
                "retry"=>i,
                "medium_access_time"=> medium_access_time,
                "wait_time"=>sleep_time.as_nanos(),
                "radio"=>&radio_range,
                "msg_id"=>msg_id,
            );
            std::thread::sleep(sleep_time);
        }

        let err_msg = String::from("Not transmitting");
        let err_cause = String::from("TRANSMISSION_MAX_RETRY reached");
        let cause = MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(err_cause),
            cause: None,
        };
        let err = MeshSimError {
            kind: MeshSimErrorKind::NetworkContention(err_msg),
            cause: Some(Box::new(cause)),
        };

        Err(err)
    }

    fn deregister_transmitter(
        &self,
        conn: &PgConnection,
        _guard: MutexGuard<()>,
    ) -> Result<(), MeshSimError> {
        // let tx = start_tx(&mut conn)?;
        let mut i = 0;
        // let wait_base = self.get_wait_base();
        let thread_id = format!("{:?}", thread::current().id());
        // self.transmitting.fetch_sub(1, Ordering::SeqCst);

        while i < TRANSMITTER_REGISTER_MAX_RETRY {
            // if self.transmitting.load(Ordering::SeqCst) > 0 {
            //     //More threads transmitting, so we shouldn't de-register the worker. Leave that to
            //     //the last thread. Just descrease the count of active threads.
            //     return Ok(());
            // }
            match remove_active_transmitter(conn, &self.worker_name, self.r_type, &self.logger) {
                Ok(_) => {
                    debug!(self.logger, "Deregistered as a transmitter");
                    // guard.drop();
                    return Ok(());
                }
                Err(e) => {
                    warn!(self.logger, "{}", &e; "Thread"=> &thread_id);
                    if let Some(cause) = e.cause {
                        warn!(self.logger, "Cause: {}", &cause);
                    }
                }
            }
            let sleep_time = self.get_wait_time(i as u32);
            warn!(self.logger, "Will Retry in {:?}", &sleep_time);
            std::thread::sleep(sleep_time);
            i += 1;
        }
        // rollback_tx(tx)?;
        let err_msg = String::from("Gave up removing worker from active_transmitter_list");
        let err = MeshSimError {
            kind: MeshSimErrorKind::SQLExecutionFailure(err_msg),
            cause: None,
        };
        Err(err)
    }

    fn get_wait_time(&self, i: u32) -> Duration {
        //CSMA-CA algorithm
        // let mut rng = self.rng.lock().expect("Could not lock RNG");
        // let r = rng.next_u64() % 2u64.pow(i);
        let mut rng = rand::thread_rng();
        let x = (rng.next_u64() % 5u64) + 1; //Can't have it be 0, otherwise the iteration is almost certainly wasted
        let r = 2u64.pow(std::cmp::min(i, TRANSMISSION_EXP_CAP));
        Duration::from_micros(self.mac_layer_base_wait * x * r)
    }
}

/// A radio object that maps directly to a network interface of the system.
#[derive(Debug)]
pub struct WifiRadio {
    ///Name of the network interface that maps to this Radio object.
    pub interface_name: String,
    ///Index of the interface
    interface_index: u32,
    ///Address that this radio listens on
    address: String,
    ///The unique id of the worker that uses this radio. The id is shared across radios belonging to the same worker.
    id: String,
    ///Name used by the worker that owns this radio.
    name: String,
    ///Random number generator used for all RNG operations.
    rng: Arc<Mutex<StdRng>>,
    ///Role this radio will take in the protocol based on it's range.
    r_type: RadioTypes,
    /// Logger for this Radio to use.
    logger: Logger,
    /// The last time this radio made a transmission
    last_transmission: Arc<AtomicI64>,
    /// Timeout for every read operation of the radio.
    timeout: u64,
}

impl Radio for WifiRadio {
    fn get_address(&self) -> &str {
        &self.address
    }

    fn get_max_loss(&self) -> f64 {
        //Value only used in simulated mode
        return -1.0f64;        
    }

    fn last_transmission(&self) -> Arc<AtomicI64> {
        // self.last_transmission.load(Ordering::SeqCst)
        Arc::clone(&self.last_transmission)
    }

    fn broadcast(&self, mut hdr: MessageHeader) -> Result<Option<TxMetadata>, MeshSimError> {
        let start_ts = Utc::now();
        let perf_out_queued_duration = start_ts.timestamp_nanos() - hdr.delay;
        let radio_range: String = self.r_type.into();
        let thread_id = format!("{:?}", thread::current().id());
        let msg_id = hdr.get_msg_id().to_string();
        let sock_addr = SocketAddr::new(IpAddr::V6(*SERVICE_ADDRESS), DNS_SERVICE_PORT);
        let socket = new_socket()?;

        //Update the hop count for the header
        hdr.hops += 1;

        socket
            .set_multicast_if_v6(self.interface_index)
            .map_err(|e| {
                let err_msg = format!(
                    "Failed to configure interface with index {}",
                    self.interface_index
                );
                MeshSimError {
                    kind: MeshSimErrorKind::Networking(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;

        let data = bincode::serialize(&hdr).map_err(|e| {
            let err_msg = String::from("Failed to serialize message");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        let packet_size = mem::size_of_val(&*data);
        let _bytes_sent = socket
            .send_to(&data, &socket2::SockAddr::from(sock_addr))
            .map_err(|e| {
                let err_msg = format!("Failed to broadcast message {}", &msg_id);
                MeshSimError {
                    kind: MeshSimErrorKind::Networking(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;
        self.last_transmission
            .store(Utc::now().timestamp_nanos(), Ordering::SeqCst);

        let perf_end_tx = Utc::now();
        //Can't measure the tx_time only in device mode
        let perf_tx_duration = 0i64;
        //Can't measure the contention only in device mode
        let perf_contention_duration = 0i64;
        let broadcast_duration = perf_end_tx.timestamp_nanos() - start_ts.timestamp_nanos();

        let tx = TxMetadata {
            radio_type: radio_range,
            thread_id,
            out_queued_duration: perf_out_queued_duration,
            contention_duration: perf_contention_duration,
            tx_duration: perf_tx_duration,
            broadcast_duration,
            size: packet_size,
            peers_in_range: 0,
        };

        // radio::log_tx(
        //     &self.logger,
        //     tx,
        //     &hdr.msg_id,
        //     MessageStatus::SENT,
        //     &hdr.sender,
        //     &hdr.destination,
        //     md,
        // );

        Ok(Some(tx))
    }
}

impl WifiRadio {
    /// Get the public address of the OS-NIC that maps to this Radio object.
    /// It will return the first IPv4 address from a NIC that exactly matches the name.
    fn get_radio_address(name: &str) -> Result<String, MeshSimError> {
        for iface in datalink::interfaces() {
            if iface.name == name {
                for address in iface.ips {
                    match address {
                        ipnetwork::IpNetwork::V4(_addr) => {
                            //return Ok(addr.ip().to_string())
                            /*Only using IPv6 for the moment*/
                        }
                        ipnetwork::IpNetwork::V6(addr) => return Ok(addr.ip().to_string()),
                    }
                }
            }
        }
        let err_msg = format!("Network interface {} not found.", name);
        let error = MeshSimError {
            kind: MeshSimErrorKind::Configuration(err_msg),
            cause: None,
        };
        Err(error)
    }

    ///Get the index of the passed interface name
    fn get_interface_index(name: &str) -> Option<u32> {
        for iface in datalink::interfaces() {
            if iface.name == name {
                return Some(iface.index);
            }
        }

        None
    }

    /// Function for creating a new DeviceRadio. It should ALWAYS be used for creating new DeviceRadios since it calculates
    ///  the address based on the DeviceRadio's properties.
    pub fn new(
        interface_name: String,
        worker_name: String,
        id: String,
        timeout: u64,
        rng: Arc<Mutex<StdRng>>,
        r_type: RadioTypes,
        logger: Logger,
    ) -> Result<(WifiRadio, Box<dyn Listener>), MeshSimError> {
        let address = WifiRadio::get_radio_address(&interface_name)?;
        let interface_index = WifiRadio::get_interface_index(&interface_name)
            .expect("Could not get index for specified interface.");
        debug!(logger, "Obtained address {}", &address);
        let address = format!("[{}]:{}", address, DNS_SERVICE_PORT);
        let listener = WifiRadio::init(interface_index, timeout, r_type, &rng, &logger)?;
        let radio = WifiRadio {
            interface_name,
            interface_index,
            id,
            name: worker_name,
            address,
            rng,
            r_type,
            logger,
            last_transmission: Default::default(),
            timeout,
        };
        Ok((radio, listener))
    }

    fn init(
        interface_index: u32,
        timeout: u64,
        r_type: RadioTypes,
        rng: &Arc<Mutex<StdRng>>,
        logger: &Logger,
    ) -> Result<Box<dyn Listener>, MeshSimError> {
        let sock = new_socket()?;

        //Join multicast group
        sock.join_multicast_v6(&SERVICE_ADDRESS, interface_index)
            .map_err(|e| {
                let err_msg = String::from("Failed to configure socket");
                MeshSimError {
                    kind: MeshSimErrorKind::Networking(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;
        sock.set_only_v6(true).map_err(|e| {
            let err_msg = String::from("Failed to configure socket");
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

        let address = SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0)),
            DNS_SERVICE_PORT,
        );
        sock.bind(&SockAddr::from(address)).map_err(|e| {
            let err_msg = format!("Could not bind socket to address {}", &address);
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        let listener = WifiListener::new(sock, timeout, Arc::clone(rng), r_type, logger.clone());

        println!("{}", &listener.get_address());
        Ok(Box::new(listener))
    }
}

//************************************************//
//*************** Utility functions **************//
//************************************************//
pub fn new_socket() -> Result<socket2::Socket, MeshSimError> {
    Socket::new(Domain::ipv6(), Type::dgram(), Some(Protocol::udp())).map_err(|e| {
        let err_msg = String::from("Failed to create new socket");
        MeshSimError {
            kind: MeshSimErrorKind::Networking(err_msg),
            cause: Some(Box::new(e)),
        }
    })
}

/// Logs outgoing packets in a standard format
pub fn log_tx(
    logger: &Logger,
    tx: TxMetadata,
    msg_id: &str,
    status: MessageStatus,
    source: &str,
    destination: &str,
    msg_metadata: ProtocolMessages,
) {
    // let debug_ts = Utc::now().to_string();
    info!(
        logger,
        "Message sent";
        msg_metadata,
        // "debug_ts" => debug_ts,
        "peers_in_range" => tx.peers_in_range,
        "size" => tx.size,
        "thread" => tx.thread_id,
        "radio" => tx.radio_type,
        "out_queued_duration" => tx.out_queued_duration,
        "contention_duration" => tx.contention_duration,
        "tx_duration" => tx.tx_duration,
        "broadcast_duration" => tx.broadcast_duration,
        "destination" => destination,
        "source" => source,
        "status" => &status,
        "msg_id"=>msg_id,
    );
}

/// Logs an incoming message
pub fn log_handle_message<T: KV>(
    logger: &Logger,
    hdr: &MessageHeader,
    status: MessageStatus,
    reason: Option<&str>,
    action: Option<&str>,
    radio: RadioTypes,
    msg: &T,
) {
    let perf_handle_message_duration = Utc::now().timestamp_nanos() - hdr.delay;
    let r_label: String = radio.into();
    info!(
        logger,
        "Received message";
        msg,
        "radio"=>r_label,
        "hops"=>hdr.hops,
        "destination"=>&hdr.destination,
        "source"=>&hdr.sender,
        "action"=>action.unwrap_or(""),
        "reason"=>reason.unwrap_or(""),
        "duration"=> perf_handle_message_duration,
        "status"=>status,
        "msg_id"=>&hdr.get_msg_id(),
    );
}

#[allow(dead_code)]
const NSS_PIN: u64 = 25;
#[allow(dead_code)]
const IRQ_PIN: u64 = 4;
#[allow(dead_code)]
const RESET_PIN: u64 = 17;

///Operation frequencies (Mhz) for Lora radios
#[derive(Debug)]
pub enum LoraFrequencies {
    ///Frequency for the USA
    USA = 915,
    ///Frequency for Europe
    Europe = 868,
    ///Frequency for other regions
    Other = 433,
}

//************************************************//
//************ Linux Implementation **************//
//************************************************//
#[cfg(target_os = "linux")]
impl<T> Radio for LoRa<T>
where
    T: 'static + Send + Sync + Link,
{
    fn get_address(&self) -> &str {
        "LoraRadio"
    }

    fn broadcast(&self, hdr: MessageHeader) -> Result<Option<TxMetadata>, MeshSimError> {
        let data = hdr.to_vec()?;
        self.transmit(data.as_slice()).map_err(|_| {
            let err_msg = String::from("Failed to transmit message");
            MeshSimError {
                kind: MeshSimErrorKind::Networking(err_msg),
                cause: None,
            }
        })?;
        let tx = Default::default();
        Ok(Some(tx))
    }

    fn last_transmission(&self) -> Arc<AtomicI64> {
        unimplemented!("LoraRadio does not yet implement last_transmission")
    }

    fn get_max_loss(&self) -> f64 {
        unimplemented!("LoraRadio does not yet implement get_max_loss")
    }
}

#[cfg(target_os = "linux")]
///Used to produce a Lora Radio trait object
pub fn new_lora_radio(
    frequency: u64,
    _spreading_factor: u32,
    transmission_power: u8,
) -> Result<Channel, MeshSimError> {
    use hal::spidev::{self, SpidevOptions};
    use hal::sysfs_gpio::Direction;
    use hal::{Pin, Spidev};

    let mut spi = Spidev::open("/dev/spidev0.0").unwrap();
    let options = SpidevOptions::new()
        .bits_per_word(8)
        .max_speed_hz(1_000_000)
        .mode(spidev::SPI_MODE_0)
        .build();
    spi.configure(&options).unwrap();

    let cs = Pin::new(NSS_PIN);
    cs.export().unwrap();
    cs.set_direction(Direction::Out).unwrap();

    let irq = Pin::new(IRQ_PIN);
    irq.export().unwrap();
    irq.set_direction(Direction::In).unwrap();

    let reset = Pin::new(RESET_PIN);
    reset.export().unwrap();
    reset.set_direction(Direction::Out).unwrap();

    let lora = LoRa::from({
        let sx1276 = sx1276::SX1276::new(spi, cs, irq, reset, frequency).unwrap();
        sx1276.set_transmission_power(transmission_power);
        sx1276
    });

    Ok((
        Arc::new(lora.clone()) as Arc<dyn Radio>,
        Box::new(lora) as Box<dyn Listener>,
    ))
}

//************************************************//
//********** Non-Linux Implementation ************//
//************************************************//
#[cfg(not(target_os = "linux"))]
///Used to produce a Lora Radio trait object
pub fn new_lora_radio(
    _frequency: u64,
    _spreading_factor: u32,
    _transmission_power: u8,
) -> Result<Channel, MeshSimError> {
    panic!("Lora radio creation is only supported on Linux");
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::logging;
    // use std::env;
    // use std::fs::File;
    // use std::io::Read;
    use crate::tests::common::*;

    #[test]
    fn test_signal_loss_calculations() {
        color_backtrace::install();
        use crate::worker::worker_config::*;
        let mut r1 = RadioConfig::new();
        let freq = r1.frequency.expect("Could not read frequency") as f64;
        let power_loss_coefficient = r1.power_loss_coefficient.expect("Could not get power_loss_coefficient");
        let reference_dist = r1.reference_distance.expect("Could not get reference distance");
        let floor_penetration_loss_factor = r1.floor_penetration_loss_factor.expect("Could not get floor_penetration_loss_factor");
        let tx_loss_at_ref_dist = 20.0f64*(freq).log(10.0) - 28.0f64;


        //Case 1 - Loss at a distance == reference_distance (1m)
        let distance = reference_dist;
        let expected_loss = 53.60422483423211;
        let loss = radio::calculate_signal_loss(
            tx_loss_at_ref_dist,
            power_loss_coefficient,
            distance,
            reference_dist,
            floor_penetration_loss_factor
        );
        assert_eq!(loss, expected_loss);

        //Case 2 - Loss at the reference distance for the empyrical wifi measurements
        let distance = 38.0;
        let expected_loss = 100.99773273273641;
        let loss = radio::calculate_signal_loss(
            tx_loss_at_ref_dist,
            power_loss_coefficient,
            distance,
            reference_dist,
            floor_penetration_loss_factor
        );
        assert_eq!(loss, expected_loss);

        //Case 3 - Distance smaller than reference distance.
        // In this case, dist should be adjusted to the minimum to not create a negative loss (a gain)
        let distance = 0.5;
        let expected_loss = 53.60422483423211;
        let loss = radio::calculate_signal_loss(
            tx_loss_at_ref_dist,
            power_loss_coefficient,
            distance,
            reference_dist,
            floor_penetration_loss_factor
        );
        assert_eq!(loss, expected_loss);

        //Case 4 - Have a reaaaallly long distance.
        let distance = f64::MAX;
        let max_possible_loss = 9301.245691631733;
        let loss = radio::calculate_signal_loss(
            tx_loss_at_ref_dist,
            power_loss_coefficient,
            distance,
            reference_dist,
            floor_penetration_loss_factor
        );
        assert_eq!(loss, max_possible_loss);
    }

    #[test]
    fn test_max_signal_loss() {
        color_backtrace::install();
        use crate::worker::worker_config::*;
        let data = setup("test_max_signal_loss", false, false);
        let mut r1 = RadioConfig::new();
        //Adjust the range to a valid number
        r1.range = 38.0;
        let freq = r1.frequency.expect("Could not read frequency") as f64;
        let power_loss_coefficient = r1.power_loss_coefficient.expect("Could not get power_loss_coefficient");
        let reference_dist = r1.reference_distance.expect("Could not get reference distance");
        let floor_penetration_loss_factor = r1.floor_penetration_loss_factor.expect("Could not get floor_penetration_loss_factor");
        let tx_loss_at_ref_dist = 20.0f64*(freq).log(10.0) - 28.0f64;
        let seed = 12345;

        let (radio, listener) = r1.create_radio(
            OperationMode::Simulated,
            RadioTypes::ShortRange,
            data.work_dir,
            String::from("Worker1"),
            String::from("foobar"),
            seed,
            None,
            data.logger.clone(),
        ).expect("Could not create radio");
        let expected_loss = 100.99773273273641;

        assert_eq!(radio.get_max_loss(), expected_loss);
    }

    #[test]
    #[should_panic]
    fn test_signal_loss_negative_dist() {
        color_backtrace::install();
        use crate::worker::worker_config::*;
        let r1 = RadioConfig::new();
        let freq = r1.frequency.expect("Could not read frequency") as f64;
        let power_loss_coefficient = r1.power_loss_coefficient.expect("Could not get power_loss_coefficient");
        let reference_dist = r1.reference_distance.expect("Could not get reference distance");
        let floor_penetration_loss_factor = r1.floor_penetration_loss_factor.expect("Could not get floor_penetration_loss_factor");
        let tx_loss_at_ref_dist = 20.0f64*(freq).log(10.0) - 28.0f64;

        let distance = -1.0;
        let expected_loss = 100.99773273273641; //irrelevant
        let loss = radio::calculate_signal_loss(
            tx_loss_at_ref_dist,
            power_loss_coefficient,
            distance,
            reference_dist,
            floor_penetration_loss_factor
        );
        assert_eq!(loss, expected_loss);
    }

    #[test]
    #[should_panic]
    fn test_signal_loss_negative_ref_dist() {
        color_backtrace::install();
        use crate::worker::worker_config::*;
        let r1 = RadioConfig::new();
        let freq = r1.frequency.expect("Could not read frequency") as f64;
        let power_loss_coefficient = r1.power_loss_coefficient.expect("Could not get power_loss_coefficient");
        let reference_dist = -1.0;
        let floor_penetration_loss_factor = r1.floor_penetration_loss_factor.expect("Could not get floor_penetration_loss_factor");
        let tx_loss_at_ref_dist = 20.0f64*(freq).log(10.0) - 28.0f64;

        let distance = 38.0;
        let expected_loss = 100.99773273273641; //irrelevant
        let loss = radio::calculate_signal_loss(
            tx_loss_at_ref_dist,
            power_loss_coefficient,
            distance,
            reference_dist,
            floor_penetration_loss_factor
        );
        assert_eq!(loss, expected_loss);
    }
}
