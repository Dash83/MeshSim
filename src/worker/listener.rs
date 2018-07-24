//! This module contains the Listener trait and its implentations. The trait hides the underlying type of
//! socket used to listen for incoming connections. This is done to abstract the protocols and worker from
//! knowing whether the worker is running on simulated or device mode.
use worker::*;
use std::thread;

/// Main trait of this module. Abstracts its underlying socket and provides methods to interact with it 
/// and listen for incoming connections.
pub trait Listener : Send {
    /// Starts listening for incoming connections on its underlying socket. Implementors must provide
    /// their own function to handle client connections.
    fn start(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError>;
}

///Listener that uses contains an underlying UnixListener. Used by the worker in simulated mode.
pub struct SimulatedListener {
    socket : UnixListener,
    delay : u32,
    reliability : f64,
    rng : Arc<Mutex<StdRng>>,
}

impl Listener for SimulatedListener {
    fn start(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError> {
        //No need for service advertisement in simulated mode.
        //Now listen for messages
        info!("Listening for messages.");
        for stream in self.socket.incoming() {
            match stream {
                Ok(s) => { 
                    //TODO: Each client connection must be handled in its own thread. Use interior-mutability pattern.
                    let client = SimulatedClient::new(s, self.delay, self.reliability, Arc::clone(&self.rng) );
                    let prot = Arc::clone(&protocol);

                    let _handle = thread::spawn(move || {
                        match SimulatedListener::handle_client(client, prot) {
                            Ok(_res) => { 
                                /* Client connection finished properly. */
                            },
                            Err(e) => {
                                error!("handle_client error: {}", e);
                            },
                        }
                    });
                },
                Err(e) => {
                    warn!("Failed to connect to incoming client. Error: {}", e);
                },
            }
        }
        Ok(())
    }
}

impl SimulatedListener {
    ///Creates a new instance of SimulatedListener
    pub fn new( socket : UnixListener, delay : u32, reliability : f64, rng : Arc<Mutex<StdRng>>) -> SimulatedListener {
        SimulatedListener{ socket : socket, 
                           delay : delay, 
                           reliability : reliability,
                           rng : rng }
    }

    ///Handles client connections
    fn handle_client(mut client : SimulatedClient, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError> {
        loop {
            //let mut data = Vec::new(); //TODO: Not sure this is the right thing here. Does the compiler optimize this allocation?
            //Read the data from the unix socket
            //let _bytes_read = try!(client_socket.read_to_end(&mut data));
            //Try to decode the data into a message.
            //let msg = try!(MessageHeader::from_vec(data));
            let msg = try!(client.read_msg());
            let response = match msg {
                Some(m) => { try!(protocol.handle_message(m)) },
                None => None,
            };

            match response {
                Some(resp_msg) => { 
                    let _res = try!(client.send_msg(resp_msg));
                },
                None => {
                    //End of protocol sequence.
                    break;
                }
            }
        }
        Ok(())
    }
}

///Listener that uses contains an underlying TCPListener. Used by the worker in simulated mode.
pub struct DeviceListener {
    socket : TcpListener,
    mdns_handler : Child,
    rng : Arc<Mutex<StdRng>>
}

impl Listener for DeviceListener {
    fn start(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError> {
        //No need for service advertisement in simulated mode.
        //Now listen for messages
        info!("Listening for messages.");
        for stream in self.socket.incoming() {
            match stream {
                Ok(s) => { 
                    let client = DeviceClient::new(s, Arc::clone(&self.rng) );
                    let prot = Arc::clone(&protocol);

                    let _handle = thread::spawn(move || {
                        match DeviceListener::handle_client(client, prot) {
                            Ok(_res) => { 
                                /* Client connection finished properly. */
                            },
                            Err(e) => {
                                error!("handle_client error: {}", e);
                            },
                        }
                    });
                },
                Err(e) => {
                    warn!("Failed to connect to incoming client. Error: {}", e);
                },
            }
        }
        Ok(())
    }   
}

impl DeviceListener {
    ///Creates a new instance of DeviceListener
    pub fn new( socket : TcpListener, h : Child, rng : Arc<Mutex<StdRng>>) -> DeviceListener {
        DeviceListener{ socket : socket,
                        mdns_handler : h,
                        rng : rng }
    }

    fn handle_client(mut client : DeviceClient, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError> {
        loop {
            //let mut data = Vec::new(); //TODO: Not sure this is the right thing here. Does the compiler optimize this allocation?
            //Read the data from the unix socket
            //let _bytes_read = try!(client_socket.read_to_end(&mut data));
            //Try to decode the data into a message.
            //let msg = try!(MessageHeader::from_vec(data));
            let msg = try!(client.read_msg());
            let response = match msg {
                Some(m) => { try!(protocol.handle_message(m)) },
                None => None,
            };

            match response {
                Some(resp_msg) => { 
                    let _res = try!(client.send_msg(resp_msg));
                },
                None => {
                    //End of protocol sequence.
                    break;
                }
            }
        }
        Ok(())
    }
}