//! Mesh simulator Worker module
//! This module defines the Worker struct, which represents one of the nodes 
//! in the Mesh deployment.
//! The worker process has the following responsibilities:
//!   1. Receive the test run-time parameters.
//!   2. Initiliazed all the specified radios with the determined transport.
//!   3. Introduce itself to the group, wait for response.
//!   4. Keep a list of peers for each radio (and one overall list).
//!   5. Send messages to any of those peers as required.
//!   6. Progagate messages receives from other peers that are not destined to itself.


// Lint options for this module
#![deny(missing_docs,
        missing_debug_implementations, missing_copy_implementations,
        trivial_casts, trivial_numeric_casts,
        unsafe_code,
        unstable_features,
        unused_import_braces, unused_qualifications)]


// *****************************
// ********** Traits **********
// *****************************
//trait message<T> {
//    fn get_sender(&self) -> Peer;
//    fn get_recipient(&self) -> Peer;
//    fn get_payload(&self) -> T;
//}


// *****************************
// ******* End traits  *********
// *****************************

// *****************************
// ********** Structs **********
// *****************************

#[derive(Debug)]
enum MessageType {
    Join,
    Ack,
    Data,
}

/// Worker struct.
/// Main struct for the worker module. Must be created via the ::new(Vec<Param>) method.
/// 
#[derive(Debug)]
pub struct Worker {
    /// Optional name for a worker. Used only for debug purposes.
    pub name: String, 
}

/// Peer struct.
/// Defines the public identity of a node in the mesh.
/// 
#[derive(Debug)]
pub struct Peer {
    /// Public key of the peer. It's considered it's public address.
    pub public_key: String, 
    /// Friendly name of the peer. 
    pub name : String,
}

/// The type of message passed as payload for Join messages.
/// The actual message is not required at this point, but used for future compatibility.
#[derive(Debug)]
pub struct JoinMessage {
    /// Optional name for a peer. Used only for debug purposes.
    name : String,
}

/// General message structured passed between workers when communicating. Depending on the
/// message type, a corresponding payload must be used.
#[derive(Debug)]
pub struct Message<T> {
     sender : Peer,
     recipient : Peer,
     payload : T,
     mtype : MessageType,

}

/// Represents a radio used by the work to send a message to the network.
#[derive(Debug)]
pub struct Radio {
    //socket : whatever socket type
    /// The transport used for the socket. In this version of the software,
    /// it's the pipe used for this radio.
    transport : String,
    /// Interference parameter used by the test. Sets a number of millisecs
    /// as base value for delay of messages. The actual delay for message sending 
    /// will be a a percentage between the constants MESSAGE_DELAY_LOW and MESSAGE_DELAY_HIGH
    interference : u32,
    /// Reliability parameter used by the test. Sets a number of percentage
    /// between (0 and 1] of probability that the message sent by this worker will
    /// not reach its destination.
    reliability : f32,

}

//impl Message<MessageType> {
//    pub fn new(mtype: MessageType) -> Message<MessageType> {
//        match mtype {
//            MessageType::Ack => { 
//                Message{sender : Peer{ public_key : "".to_string(), name : "".to_string(), payload:}, 
//                        recipient : Peer{ public_key : "".to_string(), name : "".to_string()}}
//            },
//            MessageType::Data => { },
//            MessageType::Join => { }, 
//        }
//    }
  //  
//}

impl Radio {
    /// Send a Worker::Message over the transport implemented by the current Radio.
    pub fn send<T>(&self, msg : Message<T>) -> Result<String, String> {
        Ok(String::new())
    }

    /// Constructor for new Radios
    pub fn new(trans : String, interf : u32, rel: f32) -> Radio {
        Radio{ transport : trans, interference : interf, reliability : rel }
    }
}

impl Worker {
    
    /// TODO: Add documentation for method when implementing it.
    pub fn start(&self) -> Result<String, String> {
        unimplemented!()
    }
}

// *****************************
// ******* End structs *********
// *****************************


// *****************************
// ********** Tests ************
// *****************************


#[cfg(test)]
mod tests {
    use super::*;

    static TRANSPORT1: &'static str = "ipc:///tmp/transport1.ipc";

    //**** Message unit tests ****
    #[ignore]
    #[test]
    fn dummy_test() {
        panic!("test failed!");
    }

    //**** Radio unit tests ****
    #[test]
    fn create_empty_radio() {
        let r1 = Radio{ transport : TRANSPORT1.to_string(), interference : 0, reliability: 1.0 };
        let r2 = Radio::new(TRANSPORT1.to_string(), 0, 1.0);
        assert_eq!(r1.transport, r2.transport);
        assert_eq!(r1.interference, r2.interference);
        assert_eq!(r1.reliability, r2.reliability);
    }

    //**** Peer unit tests ****
    //**** Worker unit tests ****

}

// *****************************
// ******** End tests **********
// *****************************
