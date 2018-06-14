//! This module defines the abstraction and functionality for what a Radio is in MeshSim

extern crate pnet;
extern crate ipnetwork;

use worker::*;
use std::fs;

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
pub trait Radio : std::fmt::Debug + Send {
    ///Function to create a client object to a remote peer.
    fn connect(&self, p : &Peer) -> Result<Box<Client>, WorkerError>;
    // ///Method that implements the radio-specific logic to send data over the network.
    // fn send(&self, msg : MessageHeader) -> Result<(), WorkerError>;
    ///Method that implements the radio-specific logic to scan it's medium for other nodes.
    fn scan_for_peers(&self) -> Result<HashSet<Peer>, WorkerError>;
    ///Gets the current address at which the radio is listening.
    fn get_self_peer(&self) -> &Peer;
    ///Method for the Radio to perform the necessary initialization for it to function.
    fn init(&mut self) -> Result<ListenerType, WorkerError>;
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
}

impl Radio  for SimulatedRadio {
    /// Send a Worker::Message over the address implemented by the current Radio.
    // fn send(&self, msg : MessageHeader) -> Result<(), WorkerError> {
    //     let mut socket = try!(UnixStream::connect(&msg.destination.address));
      
    //     //info!("Sending message to {}, address {}.", destination.name, destination.address);
    //     let data = try!(to_vec(&msg));
    //     try!(socket.write_all(&data));
    //     info!("Message sent successfully.");
    //     Ok(())
    // }

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

    fn init(&mut self) -> Result<ListenerType, WorkerError> {
        let mut dir = try!(std::fs::canonicalize(&self.work_dir));

        //check bcast_groups dir is there
        dir.push(SIMULATED_SCAN_DIR); //Dir is work_dir/$SIMULATED_SCAN_DIR
        if !dir.exists() {
            //Create bcast_groups dir
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
        dir.pop(); //Dir is work_dir/$SIMULATED_SCAN_DIR
        //Check if the socket file exists from a previous run.
        if Path::new(&self.me.address).exists() {
            //Pipe already exists.
            try!(fs::remove_file(&self.me.address));
        }
        let l = try!(self.get_listener());
        
        for group in groups.iter() {
            dir.push(&group); //Dir is work_dir/$SIMULATED_SCAN_DIR/&group
            
            //Does broadcast group exist?
            if !dir.exists() {
                //Create group dir
                try!(std::fs::create_dir(dir.as_path()));
                //info!("Created dir file {} ", dir.as_path().display());
            }

            //Create address or symlink for this worker
            let linked_address = format!("{}/{}.socket", dir.as_path().display(), self.me.id);
            if Path::new(&linked_address).exists() {
                //Pipe already exists
                dir.pop(); //Dir is work_dir/$SIMULATED_SCAN_DIR
                continue;
            }
            let _ = try!(std::os::unix::fs::symlink(&self.me.address, &linked_address));
            //debug!("Pipe file {} created.", &linked_address);

            dir.pop(); //Dir is work_dir/$SIMULATED_SCAN_DIR
        }

        Ok(ListenerType::Simulated(l))
    }

    fn connect(&self, p : &Peer) -> Result<Box<Client>, WorkerError> {
        let socket = try!(UnixStream::connect(&p.address));
        let client = SimulatedClient{ socket : socket};
        Ok(Box::new(client))
    }
}

impl SimulatedRadio {
    // fn send_simulated_mode(&self, msg : MessageHeader) -> Result<(), WorkerError> {
    //     let mut socket = try!(UnixStream::connect(&msg.destination.address));
      
    //     //info!("Sending message to {}, address {}.", destination.name, destination.address);
    //     let data = try!(to_vec(&msg));
    //     try!(socket.write_all(&data));
    //     info!("Message sent successfully.");
    //     Ok(())
    // }

    // fn send_device_mode(&self, msg : MessageHeader) -> Result<(), WorkerError> {
    //     let mut socket = try!(TcpStream::connect(&msg.destination.address));
        
    //     //info!("Sending message to {}, address {}.", destination.name, destination.address);
    //     let data = try!(to_vec(&msg));
    //     try!(socket.write_all(&data));
    //     info!("Message sent successfully.");
    //     Ok(())
    // }

    /// Constructor for new Radios
    pub fn new( delay : u32, 
                reliability : f64, 
                bc_groups : Vec<String>,
                work_dir : String,
                id : String,
                worker_name : String ) -> SimulatedRadio {
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
                        me : me }
    }

    ///Function for adding broadcast groups in simulated mode
    pub fn add_bcast_group(&mut self, group: String) {
        self.broadcast_groups.push(group);
    }

    fn get_listener(&self) -> Result<UnixListener, WorkerError> {
        let l = try!(UnixListener::bind(&self.me.address));
        Ok(l)
    }
}

/// A radio object that maps directly to a network interface of the system.
#[derive(Debug)]
pub struct DeviceRadio {
    ///Name of the network interface that maps to this Radio object.
    pub interface_name : String,
    ///Peer object that identifies this worker over this radio.
    pub me : Peer,
}

impl Radio  for DeviceRadio{
    // /// Send a Worker::Message over the address implemented by the current Radio.
    // fn send(&self, msg : MessageHeader) -> Result<(), WorkerError> {
    //     panic!("DeviceRadio.send() is not implemented.");
    // }

    fn scan_for_peers(&self) -> Result<HashSet<Peer>, WorkerError> {
        panic!("DeviceRadio.scan_for_peers() is not implemented.");
    }

    fn get_self_peer(&self) -> &Peer {
        &self.me
    }

    fn init(&mut self) -> Result<ListenerType, WorkerError> {
        panic!("DeviceRadio.init() is not implemented yet.");
    }

    fn connect(&self, p : &Peer) -> Result<Box<Client>, WorkerError> {
        panic!("DeviceRadio.connect() not yet implemented.");
    }
}

impl DeviceRadio {
    /// Get the public address of the OS-NIC that maps to this Radio object.
    /// It will return the first IPv4 address from a NIC that exactly matches the name.
    pub fn get_radio_address<'a>(name : &'a str) -> Result<String, WorkerError> {
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
    pub fn new(interface_name : String, worker_name : String, id : String ) -> DeviceRadio {
        let address = DeviceRadio::get_radio_address(&interface_name).expect("Could not get address for specified interface.");
        debug!("Obtained address {}", &address);
        let address = format!("{}:{}", address, DNS_SERVICE_PORT);
        let me = Peer{ id : id, 
                       name : worker_name, 
                       address : address };
        DeviceRadio { interface_name : interface_name, 
                      me : me }
    }
}

///Trait implemented by the clients returned when a radio connects to a remote peer.
pub trait Client {
    ///Sends a message to the destination specified in the msg destination using the underlying socket.
    fn send_msg(&mut self, msg : MessageHeader) -> Result<(), WorkerError>;
    ///Reads a MessageHeader from the underlying socket.
    fn read_msg(&mut self) -> Result<Option<MessageHeader>, WorkerError>;
}

/// Client returned when connecting to a remote peer under simulated mode.
pub struct SimulatedClient {
    ///Underlying socket for this client.
    pub socket : UnixStream,
}

impl Client for SimulatedClient {
    fn send_msg(&mut self, msg : MessageHeader) -> Result<(), WorkerError> {
        //info!("Sending message to {}, address {}.", destination.name, destination.address);
        let data = try!(to_vec(&msg));
        try!(self.socket.write_all(&data));
        let _ = try!(self.socket.flush());
        info!("Message sent successfully.");
        Ok(())
    }

    fn read_msg(&mut self) -> Result<Option<MessageHeader>, WorkerError> {
        let mut data : Vec<u8> = Vec::new();
        let mut total_bytes_read = 0;
        let mut result = Ok(None);

        info!("Reading message from remote peer.");

        //Read the data from the unix socket
        //let _bytes_read = try!(self.socket.read_to_end(&mut data));
        loop {
            let mut read = [0; 1024]; //Read 1kb at the time. Not particular reason for why this size.
            match self.socket.read(&mut read) {
                Ok(bytes_read) => { 
                    if bytes_read == 0 {
                        //Connection is closed.
                        debug!("Connection closed.");
                        break;
                    } else {
                        debug!("Read {} bytes from remote peer.", bytes_read);
                        data.extend_from_slice(&read);
                        total_bytes_read += bytes_read;
                        if bytes_read < read.len() {
                            //Likely that we already read all available data
                            break;
                        }
                    }
                },
                Err(e) => { 
                    error!("Error reading data from socket: {}", &e);
                    return Err(WorkerError::IO(e));
                },
            }
            debug!("It seems there's more data to read.");
        }

        //Try to decode the data into a message.
        if total_bytes_read > 0 {
            data.truncate(total_bytes_read);
            let msg = try!(MessageHeader::from_vec(data));
            result = Ok(Some(msg));
        }
        
        result
    }
}

/// Client returned when connecting to a remote peer under device mode.
pub struct DeviceClient {
    ///Underlying socket for this client.
    pub socket : TcpStream,
}

impl Client for DeviceClient {
    fn send_msg(&mut self, msg : MessageHeader) -> Result<(), WorkerError> {
        //info!("Sending message to {}, address {}.", destination.name, destination.address);
        let data = try!(to_vec(&msg));
        try!(self.socket.write_all(&data));
        let _ = try!(self.socket.flush());
        info!("Message sent successfully.");
        Ok(())
    }

    fn read_msg(&mut self) -> Result<Option<MessageHeader>, WorkerError> {
        panic!("DeviceClient.read_msg is not implemented yet.");
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
    // ///Function for scanning for nearby peers. Scanning method depends on Operation_Mode.
    // pub fn scan_for_peers(&self, t : RadioTypes) -> Result<HashSet<Peer>, WorkerError> {
    //     let radio = self.get_radio(t);
    //     match self.operation_mode {
    //         OperationMode::Simulated => { 
    //             //debug!("Scanning for peers in simulated_mode.");
    //             self.simulated_scan_for_peers(&radio)
    //         },
    //         OperationMode::Device => {
    //             //debug!("Scanning for peers in device_mode.");
    //             self.device_scan_for_peers(&radio)
    //         },
    //     }
    // }

    // fn device_scan_for_peers(&self, radio : &Radio) -> Result<HashSet<Peer>, WorkerError> {
    //     let mut peers = HashSet::new();

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
    //                     let mut p = Peer::new();
    //                     //TODO: Deconstruct these Options in a classier way. If not, might as well return emptry string on failure.
    //                     p.public_key = serv.get_txt_record("PUBLIC_KEY").unwrap_or(String::from("(NO_KEY)"));
    //                     p.name = serv.get_txt_record("NAME").unwrap_or(String::from("(NO_NAME)"));
    //                     p.address = format!("{}:{}", serv.address, DNS_SERVICE_PORT);
    //                     p.address_type = OperationMode::Device;
    //                     //p.service_record = serv;
    //                     info!("Found peer {}, address {}", p.name, p.address);
    //                     peers.insert(p);
    //                 }
    //             }
    //         }
    //     }
    //     Ok(peers)
    // }

    // fn simulated_scan_for_peers(&self, radio : &Radio) -> Result<HashSet<Peer>, WorkerError> {
    //     let mut peers = HashSet::new();

    //     //Obtain parent directory of broadcast groups
    //     let mut parent_dir = Path::new(&self.me.address).to_path_buf();
    //     let _ = parent_dir.pop(); //bcast group for main address
    //     let _ = parent_dir.pop(); //bcast group parent directory

    //     info!("Scanning for nearby peers...");
    //     debug!("Scanning in dir {}", parent_dir.display());
    //     for group in &radio.broadcast_groups {
    //         let dir = format!("{}{}{}", parent_dir.display(), std::path::MAIN_SEPARATOR, group);
    //         if Path::new(&dir).is_dir() {
    //             for path in fs::read_dir(dir)? {
    //                 let peer_file = try!(path);
    //                 let peer_file = peer_file.path();
    //                 let peer_file = peer_file.to_str().unwrap_or("");
    //                 let peer_key = extract_address_key(&peer_file);
    //                 if !peer_key.is_empty() && peer_key != self.me.public_key {
    //                     info!("Found {}!", &peer_key);
    //                     let address = format!("{}", peer_file);
    //                     let peer = Peer{name : String::from(""), 
    //                                     public_key : String::from(peer_key), 
    //                                     address : address,
    //                                     address_type : OperationMode::Simulated };
    //                     peers.insert(peer);
    //                 }
    //             }
    //         }
    //     }
    //     //Remove self from peers
    //     //let own_key = Path::new(&self.address).file_name().unwrap();
    //     //let own_key = own_key.to_str().unwrap_or("").to_string();
    //     //peers.remove(&own_key);
    //     Ok(peers)
    // }