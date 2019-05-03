//! This module defines the abstraction and functionality for what a Radio is in MeshSim

extern crate pnet;
extern crate ipnetwork;
extern crate socket2;
extern crate md5;

use worker::*;
use worker::listener::*;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use self::socket2::{Socket, SockAddr, Domain, Type, Protocol};
use std::thread::JoinHandle;
use ::slog::Logger;

const SIMULATED_SCAN_DIR : &'static str = "addr";
const SHORT_RANGE_DIR : &'static str = "short";
const LONG_RANGE_DIR : &'static str = "long";

///Maximum size the payload of a UDP packet can have.
pub const MAX_UDP_PAYLOAD_SIZE : usize = 65507; //65,507 bytes (65,535 − 8 byte UDP header − 20 byte IP header)

lazy_static! {
    ///Address used for multicast group
    pub static ref SERVICE_ADDRESS: Ipv6Addr = Ipv6Addr::new(0xFF02, 0, 0, 0, 0, 0, 0, 0x0123).into();
}

///Types of radio supported by the system. Used by Protocols that need to 
/// request an operation from the worker on a given radio.
#[derive(Debug, Clone, Copy)]
pub enum RadioTypes{
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
    type Err = WorkerError;

    fn from_str(s : &str) -> Result<RadioTypes, WorkerError> {
        //println!("Received {}", s.to_lowercase().as_str());
        let r = match s.to_lowercase().as_str() {
            LONG_RANGE_DIR => RadioTypes::LongRange,
            SHORT_RANGE_DIR => RadioTypes::ShortRange,
            &_ => return Err(WorkerError::Configuration(String::from("Unknown radio type")))
        };
        Ok(r)
    }
}
/// Trait for all types of radios.
pub trait Radio : std::fmt::Debug + Send + Sync {
    // ///Method that implements the radio-specific logic to scan it's medium for other nodes.
    // fn scan_for_peers(&self) -> Result<HashMap<String, (String, String)>, WorkerError>;
    ///Gets the current address at which the radio is listening.
    fn get_address(&self) -> &str;
    ///Used to broadcast a message using the radio
    fn broadcast(&self, hdr : MessageHeader) -> Result<(), WorkerError>;
}

/// Represents a radio used by the worker to send a message to the network.
#[derive(Debug)]
pub struct SimulatedRadio {
    /// Reliability parameter used by the test. Sets a number of percentage
    /// between (0 and 1] of probability that the message sent by this worker will
    /// not reach its destination.
    pub reliability : f64,
    ///The work dir of the worker that owns this radio.
    pub work_dir : String,
    ///Address that this radio listens on
    address : String,
    ///The unique id of the worker that uses this radio. The id is shared across radios belonging to the same worker.
    id : String,
    ///Short or long range. What's the range-role of this radio.
    r_type : RadioTypes,
    ///Range of this worker
    range : f64,
    ///Random number generator used for all RNG operations. 
    rng : Arc<Mutex<StdRng>>,
    /// Logger for this Radio to use.
    logger : Logger,
}

impl Radio  for SimulatedRadio {
    fn get_address(&self) -> &str {
        &self.address
    }

    fn broadcast(&self, hdr : MessageHeader) -> Result<(), WorkerError> {
        let conn = get_db_connection(&self.work_dir, &self.logger)?;
        let peers = get_workers_in_range(&conn, &self.id, self.range, &self.logger)?;  

        if peers.len() == 0 {
            info!(self.logger, "No nodes in range. Message not sent");
            return Ok(())
        }
        
        info!(self.logger, "{} peer in range", peers.len());
        
        let socket = Socket::new(Domain::unix(), Type::dgram(), None)?;
        let msg = hdr.clone();
        let data = to_vec(&msg)?;
        for p in peers.into_iter() {
            let selected_address = match self.r_type {
                RadioTypes::ShortRange => { p.short_address.clone() },
                RadioTypes::LongRange => { p.long_address.clone() },
            };
            
            if let Some(addr) = selected_address {
                // debug!("Sending data to {}", &addr);
                // let _res : JoinHandle<Result<(), WorkerError> > = thread::spawn(move || {
                let connect_addr : SockAddr = match SockAddr::unix(&addr) {
                    Ok(sock) => sock,
                    Err(e) => {
                        warn!(&self.logger, "Error: {}", e);
                        continue;
                    },
                };
                let _res = match socket.connect(&connect_addr) { 
                    Ok(_) => { /* All good */ },
                    Err(e) => {
                        warn!(&self.logger, "Error: {}", e);
                        continue;
                    }
                };
                let _sent_bytes = match socket.send(&data) { 
                    Ok(_) => { /* All good */ },
                    Err(e) => {
                        warn!(&self.logger, "Error: {}", e);
                        continue;
                    }
                };
                //     //info!("Message sent to {}", &peer_address);
                //     Ok(())
                // });
            } else {
                error!(self.logger, "No known address for peer peer {} in range {:?}", p.name, self.r_type);
            }
        }      
        
        info!(self.logger, "Message {:x} sent", &hdr.get_hdr_hash()?);
        Ok(())
    }

}

impl SimulatedRadio {
    /// Constructor for new Radios
    pub fn new( reliability : f64, 
                work_dir : String,
                id : String,
                worker_name : String,
                r_type : RadioTypes,
                range : f64,
                rng : Arc<Mutex<StdRng>>,
                logger : Logger ) -> Result<(SimulatedRadio, Box<Listener>), WorkerError> {
        //$WORK_DIR/SIMULATED_SCAN_DIR/ID_RANGE.sock
        let address = SimulatedRadio::format_address(&work_dir, &id, r_type);
        let listener = SimulatedRadio::init(&work_dir, &address, reliability, &rng, r_type, &logger)?;
        let sr = SimulatedRadio{reliability : reliability,
                                work_dir : work_dir,
                                id : id,
                                address : address,
                                range : range,
                                r_type : r_type,
                                rng : rng,
                                logger : logger};
        Ok((sr, listener))
    }

    ///Function used to form the listening point of a SimulatedRadio
    pub fn format_address(work_dir : &str, 
                          id : &str, 
                          r_type : RadioTypes) -> String {
        let range_dir : String = r_type.clone().into();
        format!("{}{}{}{}{}_{}.sock", work_dir, std::path::MAIN_SEPARATOR,
                                                    SIMULATED_SCAN_DIR, std::path::MAIN_SEPARATOR,
                                                    id, range_dir)
    }

    fn init(work_dir : &String,
            address : &String,
            reliability : f64,
            rng : &Arc<Mutex<StdRng>>,
            r_type : RadioTypes,
            logger : &Logger ) -> Result<Box<Listener>, WorkerError> {
        let mut dir = std::fs::canonicalize(work_dir)?;

        //Create directory for unix_domain sockets
        dir.push(SIMULATED_SCAN_DIR);
        if !dir.exists() {
            //Create bcast_groups dir
            std::fs::create_dir(dir.as_path())?;
            info!(logger, "Created dir {} ", dir.as_path().display());
        }

        let listen_addr = SockAddr::unix(address)?;
        let sock = Socket::new(Domain::unix(), Type::dgram(), None)?;
        let _ = sock.bind(&listen_addr)?;
        let listener = SimulatedListener::new(sock, 
                                              reliability, 
                                              Arc::clone(rng), 
                                              r_type,
                                              logger.clone() );
        
        println!("{}", &listener.get_address());
        Ok(Box::new(listener))
    }
}

/// A radio object that maps directly to a network interface of the system.
#[derive(Debug)]
pub struct DeviceRadio {
    ///Name of the network interface that maps to this Radio object.
    pub interface_name : String,
    ///Index of the interface
    interface_index : u32,
    ///Address that this radio listens on
    address : String,
    ///The unique id of the worker that uses this radio. The id is shared across radios belonging to the same worker.
    id : String,
    ///Name used by the worker that owns this radio.
    name : String,
    ///Random number generator used for all RNG operations. 
    rng : Arc<Mutex<StdRng>>,
    ///Role this radio will take in the protocol based on it's range.
    r_type : RadioTypes,
    /// Logger for this Radio to use.
    logger : Logger,
}

impl Radio  for DeviceRadio{
    // fn scan_for_peers(&self) -> Result<HashMap<String, (String, String)>, WorkerError> {
    //     let mut nodes_discovered = HashMap::new();

    //     //Constructing the external process call
    //     let mut command = Command::new("avahi-browse");
    //     command.arg("-r");
    //     command.arg("-p");
    //     command.arg("-t");
    //     command.arg("-l");
    //     command.arg("_http._tcp");

    //     //Starting the worker process
    //     let mut child = try!(command.stdout(Stdio::piped()).spawn());
    //     let exit_status = child.wait().unwrap();

    //     if exit_status.success() {
    //         let mut buffer = String::new();
    //         let mut output = child.stdout.unwrap();
    //         output.read_to_string(&mut buffer)?;

    //         for l in buffer.lines() {
    //             let tokens : Vec<&str> = l.split(';').collect();
    //             if tokens.len() > 6 {
    //                 let serv = ServiceRecord{ service_name : String::from(tokens[3]),
    //                                         service_type: String::from(tokens[4]), 
    //                                         host_name : String::from(tokens[6]), 
    //                                         address : String::from(tokens[7]), 
    //                                         address_type : String::from(tokens[2]), 
    //                                         port : u16::from_str_radix(tokens[8], 10).unwrap(),
    //                                         txt_records : Vec::new() };
                    
    //                 if serv.service_name.starts_with(DNS_SERVICE_NAME) {
    //                     //Found a Peer
    //                     let id = serv.get_txt_record("PUBLIC_KEY").unwrap_or(String::from("(NO_KEY)"));
    //                     let name = serv.get_txt_record("NAME").unwrap_or(String::from("(NO_NAME)"));
    //                     let address = format!("{}:{}", serv.address, DNS_SERVICE_PORT);

    //                     info!("Found peer {}, address {}", &name, &address);
    //                     if !nodes_discovered.contains_key(&id) {
    //                         nodes_discovered.insert(id, (name, address));
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //     Ok(nodes_discovered)
    // }

    fn get_address(&self) -> &str {
        &self.address
    }

    fn broadcast(&self, hdr : MessageHeader) -> Result<(), WorkerError> {
        let sock_addr = SocketAddr::new(IpAddr::V6(*SERVICE_ADDRESS), DNS_SERVICE_PORT);
        let socket = Socket::new(Domain::ipv6(), Type::dgram(), Some(Protocol::udp()))?;
        socket.set_multicast_if_v6(self.interface_index)?;
        
        let data = to_vec(&hdr)?;
        socket.send_to(&data, &socket2::SockAddr::from(sock_addr))?;

        Ok(())
    }
}

impl DeviceRadio {
    /// Get the public address of the OS-NIC that maps to this Radio object.
    /// It will return the first IPv4 address from a NIC that exactly matches the name.
    fn get_radio_address<'a>(name : &'a str) -> Result<String, WorkerError> {
        use self::pnet::datalink;
        use self::ipnetwork;

        for iface in datalink::interfaces() {
            if &iface.name == name {
                for address in iface.ips {
                    match address {
                        ipnetwork::IpNetwork::V4(_addr) => {
                            //return Ok(addr.ip().to_string())
                            /*Only using IPv6 for the moment*/ 
                        },
                        ipnetwork::IpNetwork::V6(addr) => { 
                            return Ok(addr.ip().to_string())
                        },
                    }
                }
            }
        }
        Err(WorkerError::Configuration(String::from("Network interface specified in configuration not found.")))
    }

    ///Get the index of the passed interface name
    fn get_interface_index<'a>(name : &'a str) -> Option<u32> {
        use self::pnet::datalink;

        for iface in datalink::interfaces() {
            if &iface.name == name {
                return Some(iface.index)
            }
        }

        None
    }

    /// Function for creating a new DeviceRadio. It should ALWAYS be used for creating new DeviceRadios since it calculates
    ///  the address based on the DeviceRadio's properties.
    pub fn new( interface_name : String, 
                worker_name : String, 
                id : String, 
                rng : Arc<Mutex<StdRng>>, 
                r_type : RadioTypes,
                logger : Logger ) -> Result<(DeviceRadio, Box<Listener>), WorkerError> {
        let address = DeviceRadio::get_radio_address(&interface_name).expect("Could not get address for specified interface.");
        let interface_index = DeviceRadio::get_interface_index(&interface_name).expect("Could not get index for specified interface.");
        debug!(logger, "Obtained address {}", &address);
        let address = format!("[{}]:{}", address, DNS_SERVICE_PORT);
        let listener = DeviceRadio::init(interface_index, r_type, &rng, &logger)?;
        let radio = DeviceRadio{interface_name : interface_name,
                                interface_index : interface_index,
                                id : id, 
                                name : worker_name, 
                                address : address,
                                rng : rng,
                                r_type : r_type,
                                logger : logger };
        Ok((radio, listener))
    }

    fn init(interface_index : u32,
            r_type : RadioTypes,
            rng : &Arc<Mutex<StdRng>>,
            logger : &Logger ) -> Result<Box<Listener>, WorkerError> {
        //Advertise the service to be discoverable by peers before we start listening for messages.
        // let mut service = ServiceRecord::new();
        // service.service_name = format!("{}_{}", DNS_SERVICE_NAME, self.id);
        // service.service_type = String::from(DNS_SERVICE_TYPE);
        // service.port = DNS_SERVICE_PORT;
        // service.txt_records.push(format!("PUBLIC_KEY={}", self.id));
        // service.txt_records.push(format!("NAME={}", self.name));
        // let mdns_handler = try!(ServiceRecord::publish_service(service));

        //Now bind the socket
        //debug!("Attempting to bind address: {:?}", &self.address);
        //let listen_addr = &self.address.parse::<SocketAddr>().unwrap().into();
        let sock = Socket::new(Domain::ipv6(), Type::dgram(), Some(Protocol::udp()))?;
        
        //Join multicast group
        sock.join_multicast_v6(&SERVICE_ADDRESS, interface_index)?;
        sock.set_only_v6(true)?;

        let debug_address = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0,0,0,0,0,0,0,0)), DNS_SERVICE_PORT);
        let _ = sock.bind(&SockAddr::from(debug_address))?;
        let listener = DeviceListener::new(sock, 
                                           None, 
                                           Arc::clone(rng), 
                                           r_type,
                                           logger.clone());

        println!("{}", &listener.get_address());
        Ok(Box::new(listener))
    }
}