//! This module defines the abstraction and functionality for what a Radio is in MeshSim

extern crate pnet;
extern crate ipnetwork;

use worker::*;

///Types of radio supported by the system. Used by Protocols that need to 
/// request an operation from the worker on a given radio.
#[derive(Debug)]
pub enum RadioTypes{
    ///Represents the longer-range radio amongst the available ones.
    LongRange,
    ///Represents the short-range, data radio.
    ShortRange,
}

/// Represents a radio used by the worker to send a message to the network.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Radio {
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
    ///Name of the network interface that maps to this Radio object.
    pub radio_name : String,
}

impl Radio {
    /// Send a Worker::Message over the address implemented by the current Radio.
    pub fn send(&self, msg : MessageHeader) -> Result<(), WorkerError> {
        //should the message be sent?

        //should the message be delayed?

        match &msg.destination.address_type {
            &OperationMode::Simulated => self.send_simulated_mode(msg) ,
            &OperationMode::Device => self.send_device_mode(msg),
        }
    }

    fn send_simulated_mode(&self, msg : MessageHeader) -> Result<(), WorkerError> {
        let mut socket = try!(UnixStream::connect(&msg.destination.address));
      
        //info!("Sending message to {}, address {}.", destination.name, destination.address);
        let data = try!(to_vec(&msg));
        try!(socket.write_all(&data));
        info!("Message sent successfully.");
        Ok(())
    }

    fn send_device_mode(&self, msg : MessageHeader) -> Result<(), WorkerError> {
        let mut socket = try!(TcpStream::connect(&msg.destination.address));
        
        //info!("Sending message to {}, address {}.", destination.name, destination.address);
        let data = try!(to_vec(&msg));
        try!(socket.write_all(&data));
        info!("Message sent successfully.");
        Ok(())
    }

    /// Constructor for new Radios
    pub fn new() -> Radio {
        Radio{ delay : 0,
               reliability : 1.0,
               broadcast_groups : vec![],
               radio_name : String::from("") }
    }

    ///Function for adding broadcast groups in simulated mode
    pub fn add_bcast_group(&mut self, group: String) {
        self.broadcast_groups.push(group);
    }

    ///Get the public address of the OS-NIC that maps to this Radio object.
    ///It will return the first IPv4 address from a NIC that exactly matches the name.
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
}