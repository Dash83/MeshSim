//! Mesh simulator Protocol module.
//! This module includes the important Protocol trait that needs to be implement by
//! struct to be accepted as a protocol to be run by meshsim.
use worker::{MessageHeader, WorkerError, Peer};
use self::tmembership::TMembership;
use std::collections::HashSet;
use std;

pub mod tmembership;

/// Trait that all protocols need to implement.
/// The function handle_message should 
pub trait Protocol : std::fmt::Debug {
    // ///Since the worker doesn't know about the message types that the protocols use,
    // /// it relies on this function to build messages based on the blocks of data it reads
    // /// from the network. 
    // fn build_message(&self, data : Vec<u8>) -> Result<MessageHeader, WorkerError>;
    
    
    /// This function implements the state machine of the protocol. It will match against
    /// the message type passed and call the appropriate method to handle it.
    fn handle_message(&mut self,  msg : MessageHeader) -> Result<Option<MessageHeader>, WorkerError>;

    /// Function to initialize the protocol.
    fn init_protocol(&mut self) -> Result<Option<MessageHeader>, WorkerError>;

    /// Function expoed to the worker in order to update the neaby-peers. On success, the funtion returns
    /// the wait time specified by the protocol before scheduling another peer scan.
    fn update_nearby_peers(&mut self, peers : HashSet<Peer>) -> Result<usize, WorkerError>;
    
}

// impl std::fmt::Debug for Protocol {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         write!(f, "{}", "Protocol")
//     }
// }

///Current list of supported protocols by MeshSim.
pub enum Protocols {
    ///ToyMembership protocol implemented in order to test and develop MeshSim.
    TMembership,
}

/// Provides a new boxed reference to the struct matching the passed protocol.
pub fn build_protocol_handler( p : Protocols) -> Box<Protocol> {
    match p {
        Protocols::TMembership => { 
            Box::new(TMembership::new())
        }
    }
}