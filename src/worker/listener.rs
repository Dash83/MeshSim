//! This module contains the Listener trait and its implentations. The trait hides the underlying type of
//! socket used to listen for incoming connections. This is done to abstract the protocols and worker from
//! knowing whether the worker is running on simulated or device mode.
extern crate socket2;

use worker::*;
use self::socket2::Socket;

/// Main trait of this module. Abstracts its underlying socket and provides methods to interact with it 
/// and listen for incoming connections.
pub trait Listener : Send {
    /// Starts listening for incoming connections on its underlying socket. Implementors must provide
    /// their own function to handle client connections.Uses stream-based communication.
    // fn accept_connections(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError>;
    ///Starts listening for incoming messages. This uses datagram-based communication.
    fn start(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError>;
    /// Reads a message (if possible) from the underlying socket
    fn read_message(&self) -> Option<MessageHeader>;
    /// Get's the radio-range of the current listener.
    fn get_radio_range(&self) -> RadioTypes;
}

///Listener that uses contains an underlying UnixListener. Used by the worker in simulated mode.
pub struct SimulatedListener {
    socket : Socket,
    reliability : f64,
    rng : Arc<Mutex<StdRng>>,
    r_type : RadioTypes,
    logger : Logger,
}

impl Listener for SimulatedListener {
    // fn accept_connections(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError> {
    //     //No need for service advertisement in simulated mode.
    //     //Now listen for messages
    //     let listen_addr = SockAddr::unix(&self.address)?;
    //     let socket = Socket::new(Domain::unix(), Type::stream(), None)?;
        
    //     //Bind the address
    //     let _ = socket.bind(&listen_addr)?;

    //     //Mark it as ready to start accepting connections
    //     let _ = socket.listen(MAX_CONNECTIONS)?;
    //     info!("Listening for connections");

    //     while let Ok((stream, addr)) = socket.accept() { 
    //         info!("Incoming connection from {:?}", &addr);
    //         //info!("Incoming connection");
    //         let client = SimulatedClient::new(stream, self.delay, self.reliability, Arc::clone(&self.rng) );
    //         let prot = Arc::clone(&protocol);
    //         let r_type = self.r_type;

    //         let _handle = thread::spawn(move || {
    //             match SimulatedListener::handle_client(client, prot, r_type) {
    //                 Ok(_res) => { 
    //                     /* Client connection finished properly. */
    //                 },
    //                 Err(e) => {
    //                     error!("handle_client error: {}", e);
    //                 },
    //             }
    //         });
    //     }
    //     Ok(())
    // }

    fn start(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError> {
        //let listener = UnixDatagram::bind(&self.address)?;
        let mut buffer = [0; MAX_UDP_PAYLOAD_SIZE+1];
        
        info!(self.logger, "Listening for messages");
        loop {
            match self.socket.recv_from(&mut buffer) {
                Ok((bytes_read, peer_addr)) => { 
                    info!(self.logger, "Incoming connection from {:?}", &peer_addr);
                    
                    if bytes_read > 0 {
                        let data = buffer[..bytes_read].to_vec();
                        let msg = MessageHeader::from_vec(data)?;
                        
                        //let client = SimulatedClient::new(peer_addr, self.delay, self.reliability, Arc::clone(&self.rng) );
                        let prot = Arc::clone(&protocol);
                        let r_type = self.r_type;

                        // let _handle = thread::spawn(move || {
                        //     match SimulatedListener::handle_client(data, client, prot, r_type) {
                        //         Ok(_res) => { 
                        //             /* Client connection finished properly. */
                        //         },
                        //         Err(e) => {
                        //             error!("handle_client error: {}", e);
                        //         },
                        //     }                            
                        // });
                    }
                },
                Err(e) => { 
                    warn!(self.logger, "Failed to read incoming message. Error: {}", e);
                }
            }
        }
    }

    fn read_message(&self) -> Option<MessageHeader> {
        let mut buffer = [0; MAX_UDP_PAYLOAD_SIZE+1];

        let msg = match self.socket.recv_from(&mut buffer) {
            Ok((bytes_read, _peer_addr)) => {                     
                if bytes_read > 0 {
                    let data = buffer[..bytes_read].to_vec();
                    match MessageHeader::from_vec(data) {
                        Ok(m) => { Some(m) },
                        Err(e) => { 
                            //Read bytes but could not form a MessageHeader
                            error!(self.logger, "Failed reading message: {}", e);
                            None
                        } 
                    }
                } else {
                    //0 bytes read
                    None
                }
            },
            Err(_e) => { 
                //No message read
                None
            }
        };

        msg
    }

    fn get_radio_range(&self) -> RadioTypes {
        self.r_type
    }

}

impl SimulatedListener {
    ///Creates a new instance of SimulatedListener
    pub fn new( socket : Socket, 
                reliability : f64, 
                rng : Arc<Mutex<StdRng>>, 
                r_type : RadioTypes,
                logger : Logger) -> SimulatedListener {
        SimulatedListener{ socket : socket, 
                           reliability : reliability,
                           rng : rng,
                           r_type : r_type,
                           logger : logger }
    }

    // ///Handles client connections
    // fn handle_client(mut client : SimulatedClient, protocol : Arc<Box<Protocol>>, r_type : RadioTypes) -> Result<(), WorkerError> {
    //     loop {
    //         let msg = try!(client.read_msg());
    //         let response = match msg {
    //             Some(m) => { try!(protocol.handle_message(m, r_type)) },
    //             None => None,
    //         };

    //         match response {
    //             Some(resp_msg) => { 
    //                 let _res = try!(client.send_msg(resp_msg));
    //             },
    //             None => {
    //                 //End of protocol sequence.
    //                 break;
    //             }
    //         }
    //     }
    //     Ok(())
    // }
    
    // fn handle_client(data : Vec<u8>, mut client : SimulatedClient, protocol : Arc<Box<Protocol>>, r_type : RadioTypes) -> Result<(), WorkerError> {
    //     let msg = MessageHeader::from_vec(data)?;
    //     let response = protocol.handle_message(msg, r_type)?;

    //     match response {
    //         Some(resp_msg) => { 
    //             debug!("Have a response message for {:?}", &client.peer_addr);
    //             let _res = try!(client.send_msg(resp_msg));
    //         },
    //         None => {
    //             /* No response to the incoming message */
    //         }
    //     }
    //     Ok(())
    // }
}

///Listener that uses contains an underlying TCPListener. Used by the worker in simulated mode.
pub struct DeviceListener {
    socket : Socket,
    mdns_handler : Option<Child>,
    rng : Arc<Mutex<StdRng>>,
    r_type : RadioTypes,
    logger : Logger,
}

impl Listener for DeviceListener {
    // fn accept_connections(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError> {
    //     //No need for service advertisement in simulated mode.
    //     //Now listen for messages
    //     info!("Listening for messages.");
    //     for stream in self.socket.incoming() {
    //         match stream {
    //             Ok(s) => { 
    //                 let client = DeviceClient::new(s, Arc::clone(&self.rng) );
    //                 let prot = Arc::clone(&protocol);
    //                 let r_type = self.r_type;

    //                 let _handle = thread::spawn(move || {
    //                     match DeviceListener::handle_client(client, prot, r_type) {
    //                         Ok(_res) => { 
    //                             /* Client connection finished properly. */
    //                         },
    //                         Err(e) => {
    //                             error!("handle_client error: {}", e);
    //                         },
    //                     }
    //                 });
    //             },
    //             Err(e) => {
    //                 warn!("Failed to connect to incoming client. Error: {}", e);
    //             },
    //         }
    //     }
    //     Ok(())
    // }

    fn start(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError> {
        //let listener = UnixDatagram::bind(&self.address)?;
        let mut buffer = [0; MAX_UDP_PAYLOAD_SIZE+1];
        
        info!(self.logger, "Listening for messages");
        loop {
            match self.socket.recv_from(&mut buffer) {
                Ok((bytes_read, peer_addr)) => { 
                    info!(self.logger, "Incoming connection from {:?}", &peer_addr);
                    
                    // if bytes_read > 0 {
                    //     let data = buffer[..bytes_read].to_vec();
                    //     let client = DeviceClient::new(peer_addr, Arc::clone(&self.rng));
                    //     let prot = Arc::clone(&protocol);
                    //     let r_type = self.r_type;

                    //     let _handle = thread::spawn(move || {
                    //         match DeviceListener::handle_client(data, client, prot, r_type) {
                    //             Ok(_res) => { 
                    //                 /* Client connection finished properly. */
                    //             },
                    //             Err(e) => {
                    //                 error!("handle_client error: {}", e);
                    //             },
                    //         }                            
                    //     });
                    // }
                },
                Err(e) => { 
                    warn!(self.logger, "Failed to read incoming message. Error: {}", e);
                }
            }
        }
    }

    fn read_message(&self) -> Option<MessageHeader> {
        let mut buffer = [0; MAX_UDP_PAYLOAD_SIZE+1];

        let msg = match self.socket.recv_from(&mut buffer) {
            Ok((bytes_read, _peer_addr)) => {                     
                if bytes_read > 0 {
                    let data = buffer[..bytes_read].to_vec();
                    match MessageHeader::from_vec(data) {
                        Ok(m) => { Some(m) },
                        Err(e) => { 
                            //Read bytes but could not form a MessageHeader
                            error!(self.logger, "Failed reading message: {}", e);
                            None
                        } 
                    }
                } else {
                    //0 bytes read
                    None
                }
            },
            Err(_e) => { 
                //No message read
                None
            }
        };

        msg
    }

    fn get_radio_range(&self) -> RadioTypes {
        self.r_type
    }
}

impl DeviceListener {
    ///Creates a new instance of DeviceListener
    pub fn new( socket : Socket, 
                h : Option<Child>, 
                rng : Arc<Mutex<StdRng>>, 
                r_type : RadioTypes,
                logger : Logger ) -> DeviceListener {
        DeviceListener{ socket : socket,
                        mdns_handler : h,
                        rng : rng,
                        r_type : r_type,
                        logger : logger }
    }

    // fn handle_client(data : Vec<u8>, mut client : DeviceClient, protocol : Arc<Box<Protocol>>, r_type : RadioTypes) -> Result<(), WorkerError> {
    //     let msg = MessageHeader::from_vec(data)?;
    //     let response = protocol.handle_message(msg, r_type)?;

    //     match response {
    //         Some(resp_msg) => { 
    //             debug!("Have a response message for {:?}", &client.peer_addr);
    //             let _res = try!(client.send_msg(resp_msg));
    //         },
    //         None => {
    //             /* No response to the incoming message */
    //         }
    //     }
    //     Ok(())
    // }
}