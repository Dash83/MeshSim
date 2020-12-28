//! This module contains the Listener trait and its implentations. The trait hides the underlying type of
//! socket used to listen for incoming connections. This is done to abstract the protocols and worker from
//! knowing whether the worker is running on simulated or device mode.
#[cfg(target_os = "linux")]
extern crate linux_embedded_hal as hal;

use crate::worker::radio::RadioTypes;
use crate::worker::*;
use chrono::{Duration, Utc};
use socket2::Socket;
#[cfg(target_os = "linux")]
use sx1276::socket::{Link, LoRa};

/// Main trait of this module. Abstracts its underlying socket and provides methods to interact with it
/// and listen for incoming connections.
pub trait Listener: Send + std::fmt::Debug {
    /// Starts listening for incoming connections on its underlying socket. Implementors must provide
    /// their own function to handle client connections.Uses stream-based communication.
    // fn accept_connections(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError>;
    ///Starts listening for incoming messages. This uses datagram-based communication.
    //    fn start(&self, protocol : Arc<Box<Protocol>>) -> Result<(), WorkerError>;
    /// Reads a message (if possible) from the underlying socket
    fn read_message(&self) -> Option<MessageHeader>;
    /// Get's the radio-range of the current listener.
    fn get_radio_range(&self) -> RadioTypes;
    /// Get the address at which this listener receives messages
    fn get_address(&self) -> String;
}

///Listener that uses contains an underlying UnixListener. Used by the worker in simulated mode.
#[derive(Debug)]
pub struct SimulatedListener {
    socket: Socket,
    rng: Arc<Mutex<StdRng>>,
    r_type: RadioTypes,
    logger: Logger,
}

impl Listener for SimulatedListener {
    fn read_message(&self) -> Option<MessageHeader> {
        let mut buffer = [0; MAX_UDP_PAYLOAD_SIZE + 1];
        let radio_range : String = self.r_type.into();

        match self.socket.recv_from(&mut buffer) {
            Ok((bytes_read, _peer_addr)) => {
                if bytes_read > 0 {
                    let data = buffer[..bytes_read].to_vec();
                    match MessageHeader::from_vec(data) {
                        Ok(mut m) => {
                            let ts0 = Utc::now();
                            //The counterpart of this method, SimulatedRadio::broadcast, does a fake
                            //broadcast by getting a list of nodes in range and unicasting to them
                            //one by one. This has an effect on the message propagation, as some
                            //nodes might received a message from 3 hops a way before they get it
                            //from its immediate neighbor, even if the immediate neighbor was the
                            //source. Thus, we try to amortize this by having all read_message sleep
                            //for 10ms. This should be enough time to ensure that *most* messages
                            //in the simulated broadcast have been delivered.
                            let delay = Duration::microseconds(m.delay);
                            std::thread::sleep(delay.to_std().expect("Delay was less than 0"));
                            //Update the ttl of the header
                            m.ttl -= 1;

                            //Record in the header the amount of time it took to read the message (including the delay)
                            //Useful for measuring where do the threads spend their time.
                            let dur = Utc::now().timestamp_nanos() - ts0.timestamp_nanos();

                            info!(
                                &self.logger,
                                "read_from_network";
                                "msg_id" => m.get_msg_id(),
                                "duration" => dur,
                                "radio" => &radio_range,
                            );

                            Some(m)
                        }
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
            }
            Err(_e) => {
                //No message read
                None
            }
        }
    }

    fn get_radio_range(&self) -> RadioTypes {
        self.r_type
    }

    fn get_address(&self) -> String {
        let local_address = self
            .socket
            .local_addr()
            .expect("Could not get local address");
        let v6_address = local_address
            .as_inet6()
            .expect("Could not parse address as IPv6");
        v6_address.to_string()
    }
}

impl SimulatedListener {
    ///Creates a new instance of SimulatedListener
    pub fn new(
        socket: Socket,
        timeout: u64,
        rng: Arc<Mutex<StdRng>>,
        r_type: RadioTypes,
        logger: Logger,
    ) -> SimulatedListener {
        let read_time = std::time::Duration::from_micros(timeout);
        socket
            .set_read_timeout(Some(read_time))
            // .set_nonblocking(true)
            .expect("Coult not set socket on non-blocking mode");
        SimulatedListener {
            socket,
            rng,
            r_type,
            logger,
        }
    }
}

///Listener that uses contains an underlying TCPListener. Used by the worker in simulated mode.
#[derive(Debug)]
pub struct WifiListener {
    socket: Socket,
    rng: Arc<Mutex<StdRng>>,
    r_type: RadioTypes,
    logger: Logger,
}

impl Listener for WifiListener {
    fn read_message(&self) -> Option<MessageHeader> {
        let mut buffer = [0; MAX_UDP_PAYLOAD_SIZE + 1];

        match self.socket.recv_from(&mut buffer) {
            Ok((bytes_read, _peer_addr)) => {
                if bytes_read > 0 {
                    let data = buffer[..bytes_read].to_vec();
                    match MessageHeader::from_vec(data) {
                        Ok(m) => Some(m),
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
            }
            Err(_e) => {
                //No message read
                None
            }
        }
    }

    fn get_radio_range(&self) -> RadioTypes {
        self.r_type
    }

    fn get_address(&self) -> String {
        let local_address = self
            .socket
            .local_addr()
            .expect("Could not get local address");
        let v6_address = local_address
            .as_inet6()
            .expect("Could not parse address as IPv6");
        v6_address.to_string()
    }
}

impl WifiListener {
    ///Creates a new instance of DeviceListener
    pub fn new(
        socket: Socket,
        timeout: u64,
        rng: Arc<Mutex<StdRng>>,
        r_type: RadioTypes,
        logger: Logger,
    ) -> WifiListener {
        let read_time = std::time::Duration::from_millis(timeout);
        socket
            .set_read_timeout(Some(read_time))
            .expect("Coult not set socket on non-blocking mode");
        WifiListener {
            socket,
            rng,
            r_type,
            logger,
        }
    }
}

#[cfg(target_os = "linux")]
impl<T> Listener for LoRa<T>
where
    T: 'static + Send + Sync + Link,
{
    /// Reads a message (if possible) from the underlying socket
    fn read_message(&self) -> Option<MessageHeader> {
        let mut buffer = [0; sx1276::LORA_MTU];
        loop {
            if let Ok(bytes_read) = self.receive(&mut buffer) {
                if bytes_read > 0 {
                    let data = buffer[..bytes_read].to_vec();
                    if let Ok(m) = MessageHeader::from_vec(data) {
                        return Some(m);
                    }
                }
            }
        }
    }
    /// Get's the radio-range of the current listener.
    fn get_radio_range(&self) -> RadioTypes {
        RadioTypes::LongRange
    }
    /// Get the address at which this listener receives messages
    fn get_address(&self) -> String {
        String::from("LoraRadio")
    }
}
