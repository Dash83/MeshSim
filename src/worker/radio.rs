//! This module defines the abstraction and functionality for what a Radio is in MeshSim

extern crate pnet;
extern crate ipnetwork;
extern crate socket2;
extern crate md5;

use worker::*;
use worker::listener::*;
use std::fs;
use std::process::Stdio;
use std::io::Read;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use self::socket2::{Socket, SockAddr, Domain, Type, Protocol};
use worker::rand::Rng;
use std::thread::JoinHandle;
use self::md5::Digest;

const SIMULATED_SCAN_DIR : &'static str = "bcg";
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
        println!("Received {}", s.to_lowercase().as_str());
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
    ///Method that implements the radio-specific logic to scan it's medium for other nodes.
    fn scan_for_peers(&self) -> Result<HashMap<String, (String, String)>, WorkerError>;
    ///Gets the current address at which the radio is listening.
    fn get_address(&self) -> &str;
    ///Method for the Radio to perform the necessary initialization for it to function.
    fn init(&self) -> Result<Box<Listener>, WorkerError>;
    ///Used to broadcast a message using the radio
    fn broadcast(&self, hdr : MessageHeader) -> Result<(), WorkerError>;
}

/// Represents a radio used by the worker to send a message to the network.
#[derive(Debug)]
pub struct SimulatedRadio {
    /// delay parameter used by the test. Sets a number of millisecs
    /// as base value for delay of messages. The actual delay for message sending 
    /// will be a a percentage between the constants MESSAGE_DELAY_LOW and MESSAGE_DELAY_HIGH
    pub delay : u32,
    /// Reliability parameter used by the test. Sets a number of percentage
    /// between (0 and 1] of probability that the message sent by this worker will
    /// not reach its destination.
    pub reliability : f64,
    ///Broadcast group for this radio. Only used in simulated mode.
    pub broadcast_groups : Vec<String>,
    ///The work dir of the worker that owns this radio.
    pub work_dir : String,
    ///Address that this radio listens on
    address : String,
    ///The unique id of the worker that uses this radio. The id is shared across radios belonging to the same worker.
    id : String,
    ///Short or long range. What's the range-role of this radio.
    range : RadioTypes,
    ///Random number generator used for all RNG operations. 
    rng : Arc<Mutex<StdRng>>,
}

impl Radio  for SimulatedRadio {
    fn scan_for_peers(&self) -> Result<HashMap<String, (String, String)>, WorkerError> {
        let mut nodes_discovered = HashMap::new();

        //Obtain parent directory of broadcast groups
        let mut parent_dir = Path::new(&self.address).to_path_buf();
        let _ = parent_dir.pop(); //bcast group for main address
        let _ = parent_dir.pop(); //bcast group parent directory

        info!("Scanning for peers in range: {:?}", &self.range);
        debug!("Scanning in dir {}", parent_dir.display());
        for group in &self.broadcast_groups {
            let dir = format!("{}{}{}", parent_dir.display(), std::path::MAIN_SEPARATOR, group);
            if Path::new(&dir).is_dir() {
                for path in fs::read_dir(dir)? {
                    let peer_file = try!(path);
                    let peer_file = peer_file.path();
                    let peer_file = peer_file.to_str().unwrap_or("");
                    let peer_key = extract_address_key(&peer_file);
                    if !peer_key.is_empty() && peer_key != self.id {
                        let address = format!("{}", peer_file);
                        let name = String::from("");
                        let id = String::from(peer_key);

                        if !nodes_discovered.contains_key(&id) {
                            debug!("Found {}!", &id);
                            nodes_discovered.insert(id, (name, address));
                        }
                    }
                }
            }
        }
        Ok(nodes_discovered)
    }

    fn get_address(&self) -> &str {
        &self.address
    }

    fn init(&self) -> Result<Box<Listener>, WorkerError> {
        let mut dir = try!(std::fs::canonicalize(&self.work_dir));

        //check bcast_groups dir is there
        dir.push(SIMULATED_SCAN_DIR); //Dir is work_dir/$SIMULATED_SCAN_DIR
        if !dir.exists() {
            //Create bcast_groups dir
            try!(std::fs::create_dir(dir.as_path()));
            info!("Created dir {} ", dir.as_path().display());
        }

        //Create the scan dir that corresponds to this radio's range.
        let radio_type_dir = match self.range {
            RadioTypes::ShortRange => SHORT_RANGE_DIR,
            RadioTypes::LongRange => LONG_RANGE_DIR,
        };
        dir.push(radio_type_dir);
        if !dir.exists() {
            //Create short/long dir
            try!(std::fs::create_dir(dir.as_path()));
            info!("Created dir {} ", dir.as_path().display());
        }

        //Create the main broadcast group directory and bind the socket file.
        let mut groups = self.broadcast_groups.clone();
        dir.push(groups.remove(0));
        if !dir.exists() {
            //Create main broadcast dir
            try!(std::fs::create_dir(dir.as_path()));
            info!("Created dir {} ", dir.as_path().display());
        }
        dir.pop(); //Dir is work_dir/$SIMULATED_SCAN_DIR/$RANGE
        //Check if the socket file exists from a previous run.
        if Path::new(&self.address).exists() {
            //Pipe already exists.
            try!(fs::remove_file(&self.address));
        }

        let listen_addr = SockAddr::unix(&self.address)?;
        let sock = Socket::new(Domain::unix(), Type::dgram(), None)?;
        let _ = sock.bind(&listen_addr)?;
        let listener = SimulatedListener::new( sock, self.delay, self.reliability, Arc::clone(&self.rng), self.range );
        
        for group in groups.iter() {
            dir.push(&group); //Dir is work_dir/$SIMULATED_SCAN_DIR/$RANGE/&group
            
            //Does broadcast group exist?
            if !dir.exists() {
                //Create group dir
                try!(std::fs::create_dir(dir.as_path()));
                //info!("Created dir file {} ", dir.as_path().display());
            }

            //Create address or symlink for this worker
            let linked_address = format!("{}{}{}.socket", dir.as_path().display(), std::path::MAIN_SEPARATOR, self.id);
            if Path::new(&linked_address).exists() {
                //Pipe already exists
                dir.pop(); //Dir is work_dir/$SIMULATED_SCAN_DIR/$RANGE
                continue;
            }
            let _ = try!(std::os::unix::fs::symlink(&self.address, &linked_address));
            //debug!("Pipe file {} created.", &linked_address);

            dir.pop(); //Dir is work_dir/$SIMULATED_SCAN_DIR/$RANGE
        }
        // debug!("Radio Configuration: {:?}", &self);
        Ok(Box::new(listener))
    }

    fn broadcast(&self, hdr : MessageHeader) -> Result<(), WorkerError> {
        //info!("Sending message to {}, address {}.", destination.name, destination.address);
        let r = Arc::clone(&self.rng);
        let mut rng = r.lock().unwrap();

        //Get the peers in range for broadcast
        let peers = self.scan_for_peers()?;

        for (_peer_id, (peer_name, peer_address)) in peers {
            //Check if the message will be sent
            if self.reliability < 1.0 {
                let p = rng.next_f64();

                if p > self.reliability {
                    //Message will be dropped.
                    info!("Message {:?} will not reach {}.", &hdr, &peer_name);
                }
            }
            
            let msg = hdr.clone();
            let _res : JoinHandle<Result<(), WorkerError> > = thread::spawn(move || {
                let socket = Socket::new(Domain::unix(), Type::dgram(), None)?;
                let debug_addr : SockAddr = SockAddr::unix(&peer_address)?;
                let _res = socket.connect(&debug_addr)?;
                let data = try!(to_vec(&msg));
                let _sent_bytes = socket.send(&data)?;
                //info!("Message sent to {}", &peer_address);
                Ok(())
            });
        }
        
        info!("Message {:x} sent", &hdr.get_hdr_hash()?);
        Ok(())        
    }
}

impl SimulatedRadio {
    /// Constructor for new Radios
    pub fn new( delay : u32, 
                reliability : f64, 
                bc_groups : Vec<String>,
                work_dir : String,
                id : String,
                worker_name : String,
                range : RadioTypes,
                rng : Arc<Mutex<StdRng>>,
                r_type : RadioTypes ) -> SimulatedRadio {
        let main_bcg = bc_groups[0].clone();
        //$WORK_DIR/SIMULATED_SCAN_DIR/GROUP/RANGE/ID.socket
        let range_dir :String = range.clone().into();
        let address = format!("{}{}{}{}{}{}{}{}{}.socket", work_dir, std::path::MAIN_SEPARATOR,
                                                           SIMULATED_SCAN_DIR, std::path::MAIN_SEPARATOR,
                                                           range_dir, std::path::MAIN_SEPARATOR,
                                                           main_bcg, std::path::MAIN_SEPARATOR,
                                                           id);
        SimulatedRadio{ delay : delay,
                        reliability : reliability,
                        broadcast_groups : bc_groups,
                        work_dir : work_dir,
                        id : id,
                        address : address,
                        range : range,
                        rng : rng, }
    }

    ///Function for adding broadcast groups in simulated mode
    pub fn add_bcast_group(&mut self, group: String) {
        self.broadcast_groups.push(group);
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
}

impl Radio  for DeviceRadio{
    fn scan_for_peers(&self) -> Result<HashMap<String, (String, String)>, WorkerError> {
        let mut nodes_discovered = HashMap::new();

        //Constructing the external process call
        let mut command = Command::new("avahi-browse");
        command.arg("-r");
        command.arg("-p");
        command.arg("-t");
        command.arg("-l");
        command.arg("_http._tcp");

        //Starting the worker process
        let mut child = try!(command.stdout(Stdio::piped()).spawn());
        let exit_status = child.wait().unwrap();

        if exit_status.success() {
            let mut buffer = String::new();
            let mut output = child.stdout.unwrap();
            output.read_to_string(&mut buffer)?;

            for l in buffer.lines() {
                let tokens : Vec<&str> = l.split(';').collect();
                if tokens.len() > 6 {
                    let serv = ServiceRecord{ service_name : String::from(tokens[3]),
                                            service_type: String::from(tokens[4]), 
                                            host_name : String::from(tokens[6]), 
                                            address : String::from(tokens[7]), 
                                            address_type : String::from(tokens[2]), 
                                            port : u16::from_str_radix(tokens[8], 10).unwrap(),
                                            txt_records : Vec::new() };
                    
                    if serv.service_name.starts_with(DNS_SERVICE_NAME) {
                        //Found a Peer
                        let id = serv.get_txt_record("PUBLIC_KEY").unwrap_or(String::from("(NO_KEY)"));
                        let name = serv.get_txt_record("NAME").unwrap_or(String::from("(NO_NAME)"));
                        let address = format!("{}:{}", serv.address, DNS_SERVICE_PORT);

                        info!("Found peer {}, address {}", &name, &address);
                        if !nodes_discovered.contains_key(&id) {
                            nodes_discovered.insert(id, (name, address));
                        }
                    }
                }
            }
        }
        Ok(nodes_discovered)
    }

    fn get_address(&self) -> &str {
        &self.address
    }

    fn init(&self) -> Result<Box<Listener>, WorkerError> {
        //Advertise the service to be discoverable by peers before we start listening for messages.
        let mut service = ServiceRecord::new();
        service.service_name = format!("{}_{}", DNS_SERVICE_NAME, self.id);
        service.service_type = String::from(DNS_SERVICE_TYPE);
        service.port = DNS_SERVICE_PORT;
        service.txt_records.push(format!("PUBLIC_KEY={}", self.id));
        service.txt_records.push(format!("NAME={}", self.name));
        let mdns_handler = try!(ServiceRecord::publish_service(service));

        //Now bind the socket
        //debug!("Attempting to bind address: {:?}", &self.address);
        //let listen_addr = &self.address.parse::<SocketAddr>().unwrap().into();
        let sock = Socket::new(Domain::ipv6(), Type::dgram(), Some(Protocol::udp()))?;
        
        //Join multicast group
        sock.join_multicast_v6(&SERVICE_ADDRESS, self.interface_index)?;
        sock.set_only_v6(true)?;

        let debug_address = SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0,0,0,0,0,0,0,0)), DNS_SERVICE_PORT);
        let _ = sock.bind(&SockAddr::from(debug_address))?;
        let listener = DeviceListener::new(sock, Some(mdns_handler), Arc::clone(&self.rng), self.r_type);

        info!("Radio initialized.");

        Ok(Box::new(listener))
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
                r_type : RadioTypes ) -> DeviceRadio {
        let address = DeviceRadio::get_radio_address(&interface_name).expect("Could not get address for specified interface.");
        let interface_index = DeviceRadio::get_interface_index(&interface_name).expect("Could not get index for specified interface.");
        debug!("Obtained address {}", &address);
        let address = format!("[{}]:{}", address, DNS_SERVICE_PORT);

        DeviceRadio { interface_name : interface_name,
                      interface_index : interface_index,
                      id : id, 
                      name : worker_name, 
                      address : address,
                      rng : rng,
                      r_type : r_type }
    }
}