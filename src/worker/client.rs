//!This module contains the Client trait and its implentations. The trait hides the underlying type of
//! socket used to communicate with remote hosts. This is done to abstract the protocols and worker from
//! knowing whether the worker is running on simulated or device mode.
extern crate socket2;

use worker::*;
use worker::rand::Rng;
use std::thread;
use std::time::Duration;
use std::io::Read;
use std::net::SocketAddr;
use self::socket2::{Socket, SockAddr, Domain, Type, Protocol};

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
    pub peer_addr : SockAddr,
    ///Artificial delay (in ms) introduced to sending of messages.
    pub delay : u32,
    /// Reliability parameter used by the test. Sets a number of percentage
    /// between (0 and 1] of probability that the message sent by this worker will
    /// not reach its destination.
    pub reliability : f64,
    ///Random number generator used for all RNG operations. 
    rng : Arc<Mutex<StdRng>>,    
}

impl Client for SimulatedClient {
    fn send_msg(&mut self, msg : MessageHeader) -> Result<(), WorkerError> {
        //info!("Sending message to {}, address {}.", destination.name, destination.address);
        let socket = Socket::new(Domain::unix(), Type::dgram(), None)?;
    
        //Only 1 address should be specified in the destination peer
        assert!(msg.destination.addresses.len() == 1);
        let addr = msg.destination.addresses[0].get_address();
        let debug_addr = SockAddr::unix(&addr)?;
        let _res = socket.connect(&debug_addr)?;
        
        debug!("Socket connected to peer address");

        let r = Arc::clone(&self.rng);
        let mut rng = r.lock().unwrap();
        
        //Check if the message will be sent
        if self.reliability < 1.0 {
            let p = rng.next_f64();

            if p > self.reliability {
                //Message will be dropped.
                info!("Message {:?} will be dropped.", &msg);
                return Ok(())
            }
        }

        //Check if message should be delayed.
        if self.delay > 0 {
            //Get a percerntage between 80% and 100%. The introduced delay will be p-percent
            //of the delay parameter. This is done so that the delay doesn't become a synchronized
            //delay across the simulation and actually has unexpectability about the transmission time.
            let p : f64 = rng.gen_range(0.8f64, 1.0f64);
            let delay : u64= (p * self.delay as f64).round() as u64;
            thread::sleep(Duration::from_millis(delay));
        }

        let data = try!(to_vec(&msg));
        let _sent_bytes = socket.send(&data)?;
        info!("Message sent successfully.");
        Ok(())
    }

    fn read_msg(&mut self) -> Result<Option<MessageHeader>, WorkerError> {
    //     let mut data : Vec<u8> = Vec::new();
    //     let mut total_bytes_read = 0;
    //     let mut result = Ok(None);

    //     // info!("Reading message from remote peer.");

    //     //Read the data from the unix socket
    //     //let _bytes_read = try!(self.socket.read_to_end(&mut data));
    //     loop {
    //         let mut read = [0; 1024]; //Read 1kb at the time. Not particular reason for why this size.
    //         match self.socket.read(&mut read) {
    //             Ok(bytes_read) => { 
    //                 if bytes_read == 0 {
    //                     //Connection is closed.
    //                     debug!("Connection closed.");
    //                     break;
    //                 } else {
    //                     // debug!("Read {} bytes from remote peer.", bytes_read);
    //                     data.extend_from_slice(&read);
    //                     total_bytes_read += bytes_read;
    //                     if bytes_read < read.len() {
    //                         //Likely that we already read all available data
    //                         break;
    //                     }
    //                 }
    //             },
    //             Err(e) => { 
    //                 error!("Error reading data from socket: {}", &e);
    //                 return Err(WorkerError::IO(e));
    //             },
    //         }
    //         debug!("It seems there's more data to read.");
    //     }

    //     //Try to decode the data into a message.
    //     if total_bytes_read > 0 {
    //         data.truncate(total_bytes_read);
    //         let msg = try!(MessageHeader::from_vec(data));
    //         result = Ok(Some(msg));
    //     }
        
    //     result
        unimplemented!("This function might go away...");
    }
}

impl SimulatedClient {
    ///Creates new instance of SimulatedClient
    pub fn new(addr : SockAddr, delay : u32, reliability : f64, rng : Arc<Mutex<StdRng>> ) -> SimulatedClient {
        SimulatedClient{ peer_addr : addr, delay : delay, reliability : reliability, rng : rng }
    }
}

/// Client returned when connecting to a remote peer under device mode.
pub struct DeviceClient {
    ///Underlying socket for this client.
    pub peer_addr : SockAddr,
    ///Random number generator used for all RNG operations. 
    rng : Arc<Mutex<StdRng>>,   
}

impl Client for DeviceClient {
    fn send_msg(&mut self, msg : MessageHeader) -> Result<(), WorkerError> {
        //info!("Sending message to {}, address {}.", destination.name, destination.address);
        let socket = Socket::new(Domain::ipv6(), Type::dgram(), Some(Protocol::udp()))?;
    
        //Only 1 address should be specified in the destination peer
        assert!(msg.destination.addresses.len() == 1);
        let addr = msg.destination.addresses[0].get_address();
        let remote_addr = addr.parse::<SocketAddr>().unwrap().into();
        let _res = socket.connect(&remote_addr)?;
        
        let data = to_vec(&msg)?;
        let _res = socket.send(&data)?;
        info!("Message sent successfully.");
        Ok(())
    }

    fn read_msg(&mut self) -> Result<Option<MessageHeader>, WorkerError> {
        // let mut data : Vec<u8> = Vec::new();
        // let mut total_bytes_read = 0;
        // let mut result = Ok(None);

        // info!("Reading message from remote peer.");

        // loop {
        //     let mut read = [0; 1024]; //Read 1kb at the time. Not particular reason for why this size.
        //     match self.socket.read(&mut read) {
        //         Ok(bytes_read) => { 
        //             if bytes_read == 0 {
        //                 //Connection is closed.
        //                 debug!("Connection closed.");
        //                 break;
        //             } else {
        //                 debug!("Read {} bytes from remote peer.", bytes_read);
        //                 data.extend_from_slice(&read);
        //                 total_bytes_read += bytes_read;
        //                 if bytes_read < read.len() {
        //                     //Likely that we already read all available data
        //                     break;
        //                 }
        //             }
        //         },
        //         Err(e) => { 
        //             error!("Error reading data from socket: {}", &e);
        //             return Err(WorkerError::IO(e));
        //         },
        //     }
        //     debug!("It seems there's more data to read.");
        // }

        // //Try to decode the data into a message.
        // if total_bytes_read > 0 {
        //     data.truncate(total_bytes_read);
        //     let msg = try!(MessageHeader::from_vec(data));
        //     result = Ok(Some(msg));
        // }
        
        // result
        unimplemented!("Not sure what to do with this");
    }
}

impl DeviceClient {
    ///Creates new instance of DeviceClient    
    pub fn new( addr : SockAddr, rng : Arc<Mutex<StdRng>>) -> DeviceClient {
        DeviceClient{ peer_addr : addr, 
                      rng : rng }
    }
}
