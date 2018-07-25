//! This module defines the abstraction and functionality for what a Radio is in MeshSim

extern crate pnet;
extern crate ipnetwork;

use worker::*;
use worker::listener::*;
use std::fs;
use std::process::Stdio;
use std::io::Read;

const SIMULATED_SCAN_DIR : &'static str = "bcg";
const SHORT_RANGE_DIR : &'static str = "short";
const LONG_RANGE_DIR : &'static str = "long";

///Types of radio supported by the system. Used by Protocols that need to 
/// request an operation from the worker on a given radio.
#[derive(Debug)]
pub enum RadioTypes{
    ///Represents the longer-range radio amongst the available ones.
    LongRange,
    ///Represents the short-range, data radio.
    ShortRange,
}

/// Trait for all types of radios.
pub trait Radio : std::fmt::Debug + Send + Sync {
    ///Function to create a client object to a remote peer.
    fn connect(&self, p : &Peer) -> Result<Box<Client>, WorkerError>;
    // ///Method that implements the radio-specific logic to send data over the network.
    // fn send(&self, msg : MessageHeader) -> Result<(), WorkerError>;
    ///Method that implements the radio-specific logic to scan it's medium for other nodes.
    fn scan_for_peers(&self) -> Result<HashSet<Peer>, WorkerError>;
    ///Gets the current address at which the radio is listening.
    fn get_self_peer(&self) -> &Peer;
    ///Method for the Radio to perform the necessary initialization for it to function.
    fn init(&self, t : RadioTypes) -> Result<Box<Listener>, WorkerError>;
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
    ///Peer object that identifies this worker over this radio.
    me : Peer,
    ///Short or long range. What's the range-role of this radio.
    range : RadioTypes,
    ///Random number generator used for all RNG operations. 
    rng : Arc<Mutex<StdRng>>,
}

impl Radio  for SimulatedRadio {
    fn scan_for_peers(&self) -> Result<HashSet<Peer>, WorkerError> {
        let mut peers = HashSet::new();

        //Obtain parent directory of broadcast groups
        let mut parent_dir = Path::new(&self.me.address).to_path_buf();
        let _ = parent_dir.pop(); //bcast group for main address
        let _ = parent_dir.pop(); //bcast group parent directory

        info!("Scanning for nearby peers...");
        debug!("Scanning in dir {}", parent_dir.display());
        for group in &self.broadcast_groups {
            let dir = format!("{}{}{}", parent_dir.display(), std::path::MAIN_SEPARATOR, group);
            if Path::new(&dir).is_dir() {
                for path in fs::read_dir(dir)? {
                    let peer_file = try!(path);
                    let peer_file = peer_file.path();
                    let peer_file = peer_file.to_str().unwrap_or("");
                    let peer_key = extract_address_key(&peer_file);
                    if !peer_key.is_empty() && peer_key != self.me.id {
                        info!("Found {}!", &peer_key);
                        let address = format!("{}", peer_file);
                        let peer = Peer{name : String::from(""), 
                                        id : String::from(peer_key), 
                                        address : address };
                        peers.insert(peer);
                    }
                }
            }
        }
        Ok(peers)
    }

    fn get_self_peer(&self) -> &Peer {
        &self.me
    }

    fn init(&self, t : RadioTypes) -> Result<Box<Listener>, WorkerError> {
        let mut dir = try!(std::fs::canonicalize(&self.work_dir));

        //check bcast_groups dir is there
        dir.push(SIMULATED_SCAN_DIR); //Dir is work_dir/$SIMULATED_SCAN_DIR
        if !dir.exists() {
            //Create bcast_groups dir
            try!(std::fs::create_dir(dir.as_path()));
            info!("Created dir {} ", dir.as_path().display());
        }

        //Create the scan dir that corresponds to this radio's range.
        let radio_type_dir = match t {
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
        if Path::new(&self.me.address).exists() {
            //Pipe already exists.
            try!(fs::remove_file(&self.me.address));
        }
        let l = try!(UnixListener::bind(&self.me.address));
        let listener = SimulatedListener::new( l, self.delay, self.reliability, Arc::clone(&self.rng) );
        
        for group in groups.iter() {
            dir.push(&group); //Dir is work_dir/$SIMULATED_SCAN_DIR/$RANGE/&group
            
            //Does broadcast group exist?
            if !dir.exists() {
                //Create group dir
                try!(std::fs::create_dir(dir.as_path()));
                //info!("Created dir file {} ", dir.as_path().display());
            }

            //Create address or symlink for this worker
            let linked_address = format!("{}{}{}.socket", dir.as_path().display(), std::path::MAIN_SEPARATOR, self.me.id);
            if Path::new(&linked_address).exists() {
                //Pipe already exists
                dir.pop(); //Dir is work_dir/$SIMULATED_SCAN_DIR/$RANGE
                continue;
            }
            let _ = try!(std::os::unix::fs::symlink(&self.me.address, &linked_address));
            //debug!("Pipe file {} created.", &linked_address);

            dir.pop(); //Dir is work_dir/$SIMULATED_SCAN_DIR/$RANGE
        }

        Ok(Box::new(listener))
    }

    fn connect(&self, p : &Peer) -> Result<Box<Client>, WorkerError> {
        let socket = try!(UnixStream::connect(&p.address));
        let rng = Arc::clone(&self.rng);
        let client = SimulatedClient::new(socket, self.delay, self.reliability, rng);
        Ok(Box::new(client))
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
                rng : Arc<Mutex<StdRng>> ) -> SimulatedRadio {
        let main_bcg = bc_groups[0].clone();
        //$WORK_DIR/SIMULATED_SCAN_DIR/GROUP/ID.socket
        let address = format!("{}/{}/{}/{}.socket", work_dir, SIMULATED_SCAN_DIR, main_bcg, id);
        let me = Peer{ id : id, 
                       name : worker_name, 
                       address : address };
        SimulatedRadio{ delay : delay,
                        reliability : reliability,
                        broadcast_groups : bc_groups,
                        work_dir : work_dir,
                        me : me,
                        range : range,
                        rng : rng }
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
    ///Peer object that identifies this worker over this radio.
    pub me : Peer,
    ///Random number generator used for all RNG operations. 
    rng : Arc<Mutex<StdRng>>,
}

impl Radio  for DeviceRadio{
    fn scan_for_peers(&self) -> Result<HashSet<Peer>, WorkerError> {
        let mut peers = HashSet::new();

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
                        let mut p = Peer::new();
                        //TODO: Deconstruct these Options in a classier way. If not, might as well return emptry string on failure.
                        p.id = serv.get_txt_record("PUBLIC_KEY").unwrap_or(String::from("(NO_KEY)"));
                        p.name = serv.get_txt_record("NAME").unwrap_or(String::from("(NO_NAME)"));
                        p.address = format!("{}:{}", serv.address, DNS_SERVICE_PORT);

                        info!("Found peer {}, address {}", p.name, p.address);
                        peers.insert(p);
                    }
                }
            }
        }
        Ok(peers)
    }

    fn get_self_peer(&self) -> &Peer {
        &self.me
    }

    fn init(&self, t : RadioTypes) -> Result<Box<Listener>, WorkerError> {
        //Advertise the service to be discoverable by peers before we start listening for messages.
        let mut service = ServiceRecord::new();
        service.service_name = format!("{}_{}", DNS_SERVICE_NAME, self.me.id);
        service.service_type = String::from(DNS_SERVICE_TYPE);
        service.port = DNS_SERVICE_PORT;
        service.txt_records.push(format!("PUBLIC_KEY={}", self.me.id));
        service.txt_records.push(format!("NAME={}", self.me.name));
        let mdns_handler = try!(ServiceRecord::publish_service(service));

        //Now get the TcpListener
        let l = try!(TcpListener::bind(&self.me.address));
        let listener = DeviceListener::new(l, mdns_handler, Arc::clone(&self.rng));

        Ok(Box::new(listener))
    }

    fn connect(&self, p : &Peer) -> Result<Box<Client>, WorkerError> {
        let socket = try!(TcpStream::connect(&p.address));
        let rng = Arc::clone(&self.rng);
        let client = DeviceClient::new(socket, rng);
        Ok(Box::new(client))
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
                        ipnetwork::IpNetwork::V4(addr) => {
                            return Ok(addr.ip().to_string())
                        },
                        ipnetwork::IpNetwork::V6(_) => { /*Only using IPv4 for the moment*/ },
                    }
                }
            }
        }
        Err(WorkerError::Configuration(String::from("Network interface specified in configuration not found.")))
    }

    /// Function for creating a new DeviceRadio. It should ALWAYS be used for creating new DeviceRadios since it calculates
    ///  the address based on the DeviceRadio's properties.
    pub fn new(interface_name : String, worker_name : String, id : String, rng : Arc<Mutex<StdRng>> ) -> DeviceRadio {
        let address = DeviceRadio::get_radio_address(&interface_name).expect("Could not get address for specified interface.");
        debug!("Obtained address {}", &address);
        let address = format!("{}:{}", address, DNS_SERVICE_PORT);
        let me = Peer{ id : id, 
                       name : worker_name, 
                       address : address };
        DeviceRadio { interface_name : interface_name, 
                      me : me, 
                      rng : rng }
    }
}

    //**** Radio unit tests ****
    //Unit test for: Radio::send
    //At this point, I don't know how to test this function. The function uses network connections to communciate to another process,
    //so I don't know how to mock that out in rust.
    /*
    #[test]
    fn test_radio_send() {
        unimplemented!();
    }
    */

    // //Unit test for: Radio::new
    // #[test]
    // fn test_radio_new() {
    //     let radio = Radio::new();
    //     let radio_string = "Radio { delay: 0, reliability: 1, broadcast_groups: [], radio_name: \"\" }";

    //     assert_eq!(format!("{:?}", radio), String::from(radio_string));
    // }

    // //Unit test for: Radio::add_bcast_group
    // #[test]
    // fn test_radio_add_bcast_group() {
    //     let mut radio = Radio::new();
    //     radio.add_bcast_group(String::from("group1"));

    //     assert_eq!(radio.broadcast_groups, vec![String::from("group1")]);
    // }

    //Unit test for: Radio::scan_for_peers
    //#[test]
    /*
    fn test_radio_scan_for_peers() {
        let mut worker = Worker::new();
        //3 phony groups
        worker.radios[0].add_bcast_group(String::from("group1"));
        worker.radios[0].add_bcast_group(String::from("group2"));
        worker.radios[0].add_bcast_group(String::from("group3"));

        //Create dirs
        let mut dir = Path::new("/tmp/scan_bcast_groups").to_path_buf();
        if !dir.exists() {
            let _ = fs::create_dir(&dir).unwrap();
        } else {
            //Directory structure exists. Possibly from an earlier test run.
            //Delete all directory content to ensure deterministic test results.
            let _ = fs::remove_dir_all(&dir).unwrap();
            let _ = fs::create_dir(&dir).unwrap();
        }

        dir.push("group1"); // /tmp/bcast_groups/group1
        if !dir.exists() {
            let _ = fs::create_dir(&dir).unwrap();
        }

        dir.pop();
        dir.push("group2"); // /tmp/bcast_groups/group1
        if !dir.exists() {
            let _ = fs::create_dir(&dir).unwrap();
        }

        dir.pop();
        dir.push("group3"); // /tmp/bcast_groups/group1
        if !dir.exists() {
            let _ = fs::create_dir(&dir).unwrap();
        }

        //Create address and links for this radio.
        let key_str = create_random_key();
        //Create the pipe
        let pipe = format!("/tmp/scan_bcast_groups/group1/{}.ipc", key_str);
        worker.radios[0].address = format!("ipc://{}", &pipe);
        File::create(&pipe).unwrap();
        //Create link in group 2
        let link = format!("/tmp/scan_bcast_groups/group2/{}.ipc", key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();
        //Create link in group 3
        let link = format!("/tmp/scan_bcast_groups/group3/{}.ipc", key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();

        //Create address and links for radio #2.
        let other_key_str = create_random_key();
        //Create the pipe
        let pipe = format!("/tmp/scan_bcast_groups/group2/{}.ipc", other_key_str);
        File::create(&pipe).unwrap();
        //Create link in group 2
        let link = format!("/tmp/scan_bcast_groups/group3/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();
        //Create link in group 3
        let link = format!("/tmp/scan_bcast_groups/group1/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();

        //Create address and links for radio #3.
        let other_key_str = create_random_key();
        //Create the pipe
        let pipe = format!("/tmp/scan_bcast_groups/group2/{}.ipc", other_key_str);
        File::create(&pipe).unwrap();
        //Create link in group 2
        let link = format!("/tmp/scan_bcast_groups/group3/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();
        //Create link in group 3
        let link = format!("/tmp/scan_bcast_groups/group1/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();

        //Create address and links for radio #4.
        let other_key_str = create_random_key();
        //Create the pipe
        let pipe = format!("/tmp/scan_bcast_groups/group3/{}.ipc", other_key_str);
        File::create(&pipe).unwrap();
        //Create link in group 2
        let link = format!("/tmp/scan_bcast_groups/group1/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();
        //Create link in group 3
        let link = format!("/tmp/scan_bcast_groups/group2/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();

        //Create address and links for radio #5.
        let other_key_str = create_random_key();
        //Create the pipe
        let pipe = format!("/tmp/scan_bcast_groups/group1/{}.ipc", other_key_str);
        File::create(&pipe).unwrap();
        //Create link in group 2
        let link = format!("/tmp/scan_bcast_groups/group2/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();
        //Create link in group 3
        let link = format!("/tmp/scan_bcast_groups/group3/{}.ipc", other_key_str);
        let _ = std::os::unix::fs::symlink(&pipe, &link).unwrap();

        //Scan for peers. Should find 4 peers in total.
        let peers : HashSet<Peer> = worker.scan_for_peers(&worker.radios[0]).unwrap();

        assert_eq!(peers.len(), 4);
    }
    */