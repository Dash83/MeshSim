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

#[cfg(target_os = "linux")]
use self::sx1276::socket::{Link, LoRa};
#[cfg(target_os = "linux")]
use linux_embedded_hal as hal;
#[cfg(target_os = "linux")]
use sx1276;

const SHORT_RANGE_DIR: &str = "SHORT";
const LONG_RANGE_DIR: &str = "LONG";
// const DELAY_PER_NODE: i64 = 50; //µs
const MAX_DELAY_PEERS: i64 = 10; //Maximum number of peers considered when calculating the broadcast delay
pub const DEFAULT_TRANSMISSION_MAX_RETRY: usize = 8;
const TRANSMISSION_EXP_CAP: u32 = 10;
const TRANSMITTER_REGISTER_MAX_RETRY: usize = 10;
pub const DEFAULT_TRANSMISSION_WAIT_BASE: u64 = 16; //µs

///Maximum size the payload of a UDP packet can have.
pub const MAX_UDP_PAYLOAD_SIZE: usize = 65507; //65,507 bytes (65,535 − 8 byte UDP header − 20 byte IP header)

lazy_static! {
    ///Address used for multicast group
    pub static ref SERVICE_ADDRESS: Ipv6Addr = Ipv6Addr::new(0xFF02, 0, 0, 0, 0, 0, 0, 0x0123);
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
}

/// Represents a radio used by the worker to send a message to the network.
#[derive(Debug)]
pub struct SimulatedRadio {
    /// Timeout for every read operation of the radio.
    pub timeout: u64,
    ///The work dir of the worker that owns this radio.
    pub work_dir: String,
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
    ///Maximum delay per node in a broadcast.
    max_delay_per_node: i64,
}

impl Radio for SimulatedRadio {
    fn get_address(&self) -> &str {
        &self.address
    }

    fn last_transmission(&self) -> Arc<AtomicI64> {
        // self.last_transmission.load(Ordering::SeqCst)
        Arc::clone(&self.last_transmission)
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
        let guard = self.register_transmitter(&conn);

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
                        .expect("ERROR: Cause not set for  NetworkContention event");
                    let cause_str = format!("{}", cause);
                    info!(
                        &self.logger,
                        "{}", m;
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

        //Start measuring how long it takes to perform the actual transmission
        let perf_start_tx = Utc::now();

        //Increase the hop count of the message
        hdr.hops += 1;

        //Get the list of peers in radio-range
        let peers = get_workers_in_range(&conn, &self.worker_name, self.range, &self.logger)?;
        info!(
            self.logger,
            "Starting transmission";
            "thread"=>&thread_id,
            "peers_in_range"=>peers.len(),
            "radio"=>&radio_range,
        );

        let socket = new_socket()?;
        let mut acc_delay: i64 = 0;
        let max_delay = std::cmp::min(MAX_DELAY_PEERS, peers.len() as i64) * self.max_delay_per_node;
        let delay_per_node = max_delay / std::cmp::max(1, peers.len()) as i64;
        for p in peers.into_iter() {
            let mut msg = hdr.clone();
            msg.delay = max_delay - acc_delay;
            acc_delay += delay_per_node;
            let data = to_vec(&msg).map_err(|e| {
                let err_msg = String::from("Failed to serialize message");
                MeshSimError {
                    kind: MeshSimErrorKind::Serialization(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;

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

        //Update last transmission time
        self.last_transmission
            .store(Utc::now().timestamp_nanos(), Ordering::SeqCst);
        debug!(
            &self.logger,
            "last_transmission:{}",
            self.last_transmission.load(Ordering::SeqCst)
        );

        self.deregister_transmitter(&conn, guard)?;
        let perf_end_tx = Utc::now();
        //Time it took for the actual transmission to execute (e.g. iteratiing through the peers and sending the messages)
        let perf_tx_duration = perf_end_tx.timestamp_nanos() - perf_start_tx.timestamp_nanos();
        //Time it took for the whole function to execute
        let broadcast_duration = perf_end_tx.timestamp_nanos() - start_ts.timestamp_nanos();

        let tx = TxMetadata {
            radio_type: radio_range,
            thread_id,
            out_queued_duration: perf_out_queued_duration,
            contention_duration: perf_contention_duration,
            tx_duration: perf_tx_duration,
            broadcast_duration,
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
        max_delay_per_node: i64,
        rng: Arc<Mutex<StdRng>>,
        logger: Logger,
    ) -> Result<(SimulatedRadio, Box<dyn Listener>), MeshSimError> {
        // let address = SimulatedRadio::format_address(&work_dir, &id, r_type);
        let listener = SimulatedRadio::init(timeout, &rng, r_type, &logger)?;
        let listen_addres = listener.get_address();
        let sr = SimulatedRadio {
            timeout,
            work_dir,
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
            max_delay_per_node,
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

    ///Receives the output from a worker that initialized 1 or more radios, and returns
    ///the respective listen addresses.
    // pub fn extract_radio_addresses(
    //     mut input: String,
    //     logger: &Logger,
    // ) -> Result<(Option<String>, Option<String>, Option<String>), MeshSimError> {
    //     let mut sr_address = None;
    //     let mut lr_address = None;
    //     let mut cmd_address: Option<String> = None;

    //     // If the last char is a new line, remove it.
    //     let last_c = input.pop();
    //     if let Some(c) = last_c {
    //         if c != '\n' {
    //             input.push(c);
    //         }
    //     }

    //     for v in input.as_str().split(';') {
    //         if v.is_empty() {
    //             continue;
    //         }
    //         let parts: Vec<&str> = v.split('-').collect();
    //         debug!(logger, "Parts: {:?}", &parts);
    //         if parts[0] == "SHORT" {
    //             // RadioTypes::ShortRange
    //             sr_address = Some(parts[1].into());
    //         } else if parts[0] == "LONG" {
    //             // RadioTypes::LongRange
    //             lr_address = Some(parts[1].into());
    //         } else if parts[0] == "Command" {
    //             cmd_address = Some(parts[1].into());
    //         } else {
    //             let err_msg = format!("Could not parse radio-address {}", &input);
    //             let err = MeshSimError {
    //                 kind: MeshSimErrorKind::Configuration(err_msg),
    //                 cause: None,
    //             };
    //             return Err(err);
    //         }

    //         // debug!(logger, "RadioType {:?}", &r_type);
    //         // match r_type {
    //         //     RadioTypes::ShortRange => {
    //         //         sr_address = Some(parts[1].into());
    //         //     }
    //         //     RadioTypes::LongRange => {
    //         //         lr_address = Some(parts[1].into());
    //         //     }
    //         // }
    //     }
    //     // debug!(logger, "This function did finish!");

    //     Ok((sr_address, lr_address, cmd_address))
    // }

    fn register_transmitter(&self, conn: &PgConnection) -> Result<MutexGuard<()>, MeshSimError> {
        // let tx = start_tx(&mut conn)?;
        let mut i = 0;
        // let wait_base = self.get_wait_base();
        let thread_id = format!("{:?}", thread::current().id());
        let radio_range: String = self.r_type.into();

        //Only one thread can transmit simultaneously, as in real life, the NIC can broadcast more 
        //than one signal concurrently.
        let guard = self
            .transmitting
            .lock()
            .expect("Could not acquire transmitter lock");

        //The config text mac_base_REtry, so it should retry up to that number (the initial attempt is not a retry.)
        while i <= self.mac_layer_retries {

            match register_active_transmitter_if_free(
                conn,
                &self.worker_name,
                self.r_type,
                self.range,
                &self.logger,
            ) {
                Ok(rows) => {
                    if rows > 0 {
                        debug!(
                            &self.logger,
                            "{} registered as an active transmitter for radio {}",
                            &self.worker_name,
                            &radio_range;
                            "thread"=>&thread_id
                        );
                        //Increase the count of transmitting threads
                        // self.transmitting.fetch_add(1, Ordering::SeqCst);

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
            i += 1;
            let sleep_time = self.get_wait_time(i as u32);
            // let st = format!("{:?}", &sleep_time);
            info!(
                &self.logger,
                "Medium is busy";
                "thread"=>&thread_id,
                "retry"=>i,
                "wait_time"=>sleep_time.as_nanos(),
                "radio"=>&radio_range,
            );
            std::thread::sleep(sleep_time);
        }

        let err_msg = String::from("Aborting transmission");
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

    // fn get_wait_base(&self) -> u64 {
    //     let mut rng = self.rng.lock().expect("Could not lock RNG");
    //     (rng.next_u64() % WAIT_BASE_SERIES_LIMIT) + MIN_WAIT_BASE
    // }

    fn get_wait_time(&self, i: u32) -> Duration {
        //CSMA-CA algorithm
        // let mut rng = self.rng.lock().expect("Could not lock RNG");
        // let r = rng.next_u64() % 2u64.pow(i);
        let mut rng = rand::thread_rng();
        let x = rng.next_u64() % 5u64;
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

        let data = to_vec(&hdr).map_err(|e| {
            let err_msg = String::from("Failed to serialize message");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
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
        let data = to_vec(&hdr).map_err(|e| {
            let err_msg = String::from("Failed to serialize message");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
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
