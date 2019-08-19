//! This module defines the abstraction and functionality for what a Radio is in MeshSim

use crate::worker::listener::*;
use crate::worker::mobility::*;
use crate::worker::*;
use crate::{MeshSimError, MeshSimErrorKind};
use pnet_datalink as datalink;
use slog::Logger;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::time::Duration;
use rand::RngCore;
use std::str::FromStr;

#[cfg(target_os = "linux")]
use self::sx1276::socket::{Link, LoRa};
#[cfg(target_os = "linux")]
use linux_embedded_hal as hal;
#[cfg(target_os = "linux")]
use sx1276;

//const SIMULATED_SCAN_DIR : &'static str = "addr";
const SHORT_RANGE_DIR: &str = "SHORT";
const LONG_RANGE_DIR: &str = "LONG";
const DELAY_PER_NODE: u64 = 50; //microseconds
const TRANSMISSION_MAX_RETRY: usize = 10;

///Maximum size the payload of a UDP packet can have.
pub const MAX_UDP_PAYLOAD_SIZE: usize = 65507; //65,507 bytes (65,535 − 8 byte UDP header − 20 byte IP header)

lazy_static! {
    ///Address used for multicast group
    pub static ref SERVICE_ADDRESS: Ipv6Addr = Ipv6Addr::new(0xFF02, 0, 0, 0, 0, 0, 0, 0x0123);
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
    fn broadcast(&self, hdr: MessageHeader) -> Result<(), MeshSimError>;
}

/// Represents a radio used by the worker to send a message to the network.
#[derive(Debug)]
pub struct SimulatedRadio {
    /// Reliability parameter used by the test. Sets a number of percentage
    /// between (0 and 1] of probability that the message sent by this worker will
    /// not reach its destination.
    pub reliability: f64,
    ///The work dir of the worker that owns this radio.
    pub work_dir: String,
    ///Address that this radio listens on
    address: String,
    ///Name of the node that owns this radio
    worker_name : String,
    ///The unique id of the worker that uses this radio. The id is shared across radios belonging to the same worker.
    id: String,
    ///Short or long range. What's the range-role of this radio.
    r_type: RadioTypes,
    ///Range of this worker
    range: f64,
    ///Random number generator used for all RNG operations.
    rng: Arc<Mutex<StdRng>>,
    /// Logger for this Radio to use.
    logger: Logger,
}

impl Radio for SimulatedRadio {
    fn get_address(&self) -> &str {
        &self.address
    }

    fn broadcast(&self, hdr: MessageHeader) -> Result<(), MeshSimError> {
        let mut conn = get_db_connection(&self.work_dir, &self.logger)?;
        let mut peers = Vec::new();
        let mut i = 0;
        let wait_base = {
            let mut rng = self.rng.lock().expect("Could not lock RNG");
            (rng.next_u64() % 4) + 1
        };

        while i < TRANSMISSION_MAX_RETRY {
            let tx = start_tx(&mut conn)?;

            //Get peers in range
            peers = get_workers_in_range(&tx, &self.id, self.range, &self.logger)?;

            //If no peer is in range, we are done here
            if peers.is_empty() {
                info!(self.logger, "No nodes in range. Message not sent");
                return Ok(());
            }

            //Get the list of all active transmitters
            let active_transmitters = get_active_transmitters(
                &tx,
                self.r_type,
                &self.logger
            )?;

            //How many active transmitters are in range? If more than 0, the medium is busy and we
            //have to go into exponential back-off.
            let peer_names = HashSet::from_iter(peers.iter().map(|x| x.name.clone()));
            let active_transmitters_in_range: HashSet<&String> = active_transmitters
                .intersection(&peer_names)
                .collect();

            debug!(self.logger, "{} active transmitters in range", active_transmitters_in_range.len());

            if active_transmitters_in_range.len() == 0 {
                break;
            }

            //Error out?
            if i == TRANSMISSION_MAX_RETRY {
                let err_msg = String::from("TRANSMISSION_MAX_RETRY reached. Aborting transmission");
                let err = MeshSimError{
                    kind : MeshSimErrorKind::Networking(err_msg),
                    cause : None,
                };
                return Err(err)
            }

            //Exponential back-off
            info!(&self.logger, "Medium is busy");
            i += 1;
            let sleep_time = Duration::from_micros(wait_base.pow(i as u32));
            info!(&self.logger, "Retrying in {:?}", &sleep_time);
            std::thread::sleep(sleep_time);
        }

        info!(self.logger, "Starting transmission");
        let tx = start_tx(&mut conn)?;

        let _ = insert_active_transmitter(&tx,
                                          &self.worker_name,
                                          self.r_type,
                                          &self.logger)?;

        let _ = commit_tx(tx)?;

        debug!(&self.logger, "{} registered as an active transmitter", &self.worker_name);
        info!(self.logger, "{} peers in range", peers.len());

        // let socket = Socket::new(Domain::unix(), Type::dgram(), None)?;
        let socket = new_socket()?;
        let mut delay: u64 = 0;
        let max_delay = peers.len() as u64 * DELAY_PER_NODE;
        for p in peers.into_iter() {
            let mut msg = hdr.clone();
            msg.delay = max_delay - delay;
            delay += DELAY_PER_NODE;
            let data = to_vec(&msg).map_err(|e| {
                let err_msg = String::from("Failed to serialize message");
                MeshSimError {
                    kind: MeshSimErrorKind::Serialization(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;

            let selected_address = match self.r_type {
                RadioTypes::ShortRange => p.short_address.clone(),
                RadioTypes::LongRange => p.long_address.clone(),
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
                    "No known address for peer {} in range {:?}", p.name, self.r_type
                );
            }
        }

        let tx = start_tx(&mut conn)?;

        //Remove from active-transmitters list
        let _ = remove_active_transmitter(&tx,
                                          &self.worker_name,
                                          self.r_type,
                                          &self.logger)?;

        let _ = commit_tx(tx)?;

        info!(self.logger, "Message {:x} sent", &hdr.get_hdr_hash());
        Ok(())
    }
}

impl SimulatedRadio {
    /// Constructor for new Radios
    pub fn new(
        reliability: f64,
        work_dir: String,
        id: String,
        worker_name: String,
        r_type: RadioTypes,
        range: f64,
        rng: Arc<Mutex<StdRng>>,
        logger: Logger,
    ) -> Result<(SimulatedRadio, Box<dyn Listener>), MeshSimError> {
        // let address = SimulatedRadio::format_address(&work_dir, &id, r_type);
        let listener = SimulatedRadio::init(reliability, &rng, r_type, &logger)?;
        let listen_addres = listener.get_address();
        let sr = SimulatedRadio {
            reliability,
            work_dir,
            id,
            worker_name,
            address: listen_addres,
            range,
            r_type,
            rng,
            logger,
        };
        Ok((sr, listener))
    }

    // ///Function used to form the listening point of a SimulatedRadio
    // pub fn format_address(work_dir : &str,
    //                       id : &str,
    //                       r_type : RadioTypes) -> String {
    //     let range_dir : String = r_type.clone().into();
    //     //$WORK_DIR/SIMULATED_SCAN_DIR/ID_RANGE.sock
    //     format!("{}{}{}{}{}_{}.sock", work_dir, std::path::MAIN_SEPARATOR,
    //                                                 SIMULATED_SCAN_DIR, std::path::MAIN_SEPARATOR,
    //                                                 id, range_dir)
    // }

    fn init(
        reliability: f64,
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
            SimulatedListener::new(sock, reliability, Arc::clone(rng), r_type, logger.clone());
        let radio_type: String = r_type.into();
        print!("{}-{};", radio_type, &listener.get_address());

        Ok(Box::new(listener))
    }

    ///Receives the output from a worker that initialized 1 or more radios, and returns
    ///the respective listen addresses.
    pub fn extract_radio_addresses(
        mut input: String,
        logger : &Logger
    ) -> Result<(Option<String>, Option<String>), MeshSimError> {
        let mut sr_address = None;
        let mut lr_address = None;

        // If the last char is a new line, remove it.
        let last_c = input.pop();
        if let Some(c) = last_c {
            if c != '\n' {
                input.push(c);
            }
        }

        for v in input.as_str().split(';') {
            if v.is_empty() {
                continue;
            }
            let parts: Vec<&str> = v.split('-').collect();
            debug!(logger, "Parts: {:?}", &parts);
            let r_type = {
                if parts[0] == "SHORT" {
                    RadioTypes::ShortRange
                } else if parts[0] == "LONG" {
                    RadioTypes::LongRange
                } else {
                    let err_msg = format!("Could not parse radio-address {}", &input);
                    let err = MeshSimError {
                        kind: MeshSimErrorKind::Configuration(err_msg),
                        cause: None,
                    };
                    return Err(err)
                }
            };
//            let r_type: RadioTypes = parts[0].parse().map_err(|e| {
//                let err_msg = format!("Could not parse radio-address {}", &input);
//                MeshSimError {
//                    kind: MeshSimErrorKind::Configuration(err_msg),
//                    cause: Some(Box::new(e)),
//                }
//            })?;
            debug!(logger, "RadioType {:?}", &r_type);
            match r_type {
                RadioTypes::ShortRange => {
                    sr_address = Some(parts[1].into());
                }
                RadioTypes::LongRange => {
                    lr_address = Some(parts[1].into());
                }
            }
        }
        debug!(logger, "This function did finish!");

        Ok((sr_address, lr_address))
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
}

impl Radio for WifiRadio {
    fn get_address(&self) -> &str {
        &self.address
    }

    fn broadcast(&self, hdr: MessageHeader) -> Result<(), MeshSimError> {
        let sock_addr = SocketAddr::new(IpAddr::V6(*SERVICE_ADDRESS), DNS_SERVICE_PORT);
        let socket = new_socket()?;
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
                let err_msg = format!("Failed to broadcast message {:x}", hdr.get_hdr_hash());
                MeshSimError {
                    kind: MeshSimErrorKind::Networking(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?;

        Ok(())
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
        rng: Arc<Mutex<StdRng>>,
        r_type: RadioTypes,
        logger: Logger,
    ) -> Result<(WifiRadio, Box<dyn Listener>), MeshSimError> {
        let address = WifiRadio::get_radio_address(&interface_name)?;
        let interface_index = WifiRadio::get_interface_index(&interface_name)
            .expect("Could not get index for specified interface.");
        debug!(logger, "Obtained address {}", &address);
        let address = format!("[{}]:{}", address, DNS_SERVICE_PORT);
        let listener = WifiRadio::init(interface_index, r_type, &rng, &logger)?;
        let radio = WifiRadio {
            interface_name,
            interface_index,
            id,
            name: worker_name,
            address,
            rng,
            r_type,
            logger,
        };
        Ok((radio, listener))
    }

    fn init(
        interface_index: u32,
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
        let listener = WifiListener::new(sock, None, Arc::clone(rng), r_type, logger.clone());

        println!("{}", &listener.get_address());
        Ok(Box::new(listener))
    }
}

//************************************************//
//*************** Utility functions **************//
//************************************************//
fn new_socket() -> Result<socket2::Socket, MeshSimError> {
    Socket::new(Domain::ipv6(), Type::dgram(), Some(Protocol::udp())).map_err(|e| {
        let err_msg = String::from("Failed to create new socket");
        MeshSimError {
            kind: MeshSimErrorKind::Networking(err_msg),
            cause: Some(Box::new(e)),
        }
    })
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

    fn broadcast(&self, hdr: MessageHeader) -> Result<(), MeshSimError> {
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
        Ok(())
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
