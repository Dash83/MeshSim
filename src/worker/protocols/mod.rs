//! Mesh simulator Protocol module.
//! This module includes the important Protocol trait that needs to be implement by
//! struct to be accepted as a protocol to be run by meshsim.
use worker::{MessageHeader, WorkerError};
use self::tmembership::TMembership;

pub mod tmembership;

/// Trait that all protocols need to implement.
/// The function handle_message should 
pub trait Protocol {
    // ///Since the worker doesn't know about the message types that the protocols use,
    // /// it relies on this function to build messages based on the blocks of data it reads
    // /// from the network. 
    // fn build_message(&self, data : Vec<u8>) -> Result<MessageHeader, WorkerError>;
    
    
    /// This function implements the state machine of the protocol. It will match against
    /// the message type passed and call the appropriate method to handle it.
    fn handle_message(&self,  msg : MessageHeader) -> Option<MessageHeader>;
    
}

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