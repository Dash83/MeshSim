//! Mesh simulator Protocol module.
//! This module includes the important Protocol trait that needs to be implement by
//! struct to be accepted as a protocol to be run by meshsim.
use worker::{MessageHeader, WorkerError, Radio};
use self::tmembership::TMembership;
use std;

pub mod tmembership;

/// Trait that all protocols need to implement.
/// The function handle_message should 
pub trait Protocol : std::fmt::Debug {
    /// This function implements the state machine of the protocol. It will match against
    /// the message type passed and call the appropriate method to handle it.
    fn handle_message(&mut self,  msg : MessageHeader) -> Result<Option<MessageHeader>, WorkerError>;

    /// Function to initialize the protocol.
    fn init_protocol(&mut self) -> Result<Option<MessageHeader>, WorkerError>;

}

///Current list of supported protocols by MeshSim.
pub enum Protocols {
    ///ToyMembership protocol implemented in order to test and develop MeshSim.
    TMembership,
}

/// Provides a new boxed reference to the struct matching the passed protocol.
pub fn build_protocol_handler( p : Protocols, sr : Box<Radio> ) -> Box<Protocol> {
    match p {
        Protocols::TMembership => { 
            Box::new(TMembership::new(sr))
        }
    }
}