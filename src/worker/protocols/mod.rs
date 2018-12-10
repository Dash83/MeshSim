//! Mesh simulator Protocol module.
//! This module includes the important Protocol trait that needs to be implement by
//! struct to be accepted as a protocol to be run by meshsim.

extern crate rand;
extern crate toml;
extern crate rustc_serialize;

use worker::{MessageHeader, WorkerError };
use worker::radio::*;
use worker::listener::*;
use self::tmembership::TMembership;
use self::tmembership_advanced::*;
use self::naive_routing::*;
use std;
use std::sync::Arc;
use std::str::FromStr;

pub mod tmembership;
pub mod tmembership_advanced;
pub mod naive_routing;

/// Trait that all protocols need to implement.
/// The function handle_message should 
pub trait Protocol : std::fmt::Debug + Send + Sync {
    /// This function implements the state machine of the protocol. It will match against
    /// the message type passed and call the appropriate method to handle it.
    fn handle_message(&self,  msg : MessageHeader, r_type : RadioTypes) -> Result<Option<MessageHeader>, WorkerError>;

    /// Function to initialize the protocol.
    fn init_protocol(&self) -> Result<Option<MessageHeader>, WorkerError>;

    /// Function to send command to another node in the network
    fn send(&self, destination : String, data : Vec<u8>) -> Result<(), WorkerError>;

}

///Current list of supported protocols by MeshSim.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
pub enum Protocols {
    /// ToyMembership protocol implemented in order to test and develop MeshSim.
    TMembership,
    /// 2-Radio variation of the TMembership protocol.
    TMembershipAdvanced,
    /// Broadcast based routing
    NaiveRouting,
}

impl FromStr for Protocols {
    type Err = WorkerError;

    fn from_str(s : &str) -> Result<Protocols, WorkerError> {
        match s.to_uppercase().as_str() {
            "TMEMBERSHIP" => Ok(Protocols::TMembership),
            "TMEMBERSHIP_ADVANCED" => Ok(Protocols::TMembershipAdvanced),
            "NAIVEROUTING" => Ok(Protocols::NaiveRouting),
            _ => Err(WorkerError::Configuration(String::from("The specified protocol is not supported."))),
        }
    }
}

/// Helper struct for the return type of build_protocol_resources().
/// It contains all the protocol-releated resources necessary for the worker to do it's job.
pub struct ProtocolResources {
    ///Protocol handler for the selected protocol.
    pub handler : Arc<Protocol>,
    ///Collection of rx/tx radio channels for the worker and protocol to communicate.
    pub radio_channels : Vec<(Box<Listener>, Arc<Radio>)>,
}

/// Provides a new boxed reference to the struct matching the passed protocol.
pub fn build_protocol_resources( p : Protocols, 
                                 short_radio : Option<Arc<Radio>>,
                                 long_radio : Option<Arc<Radio>>,
                                 seed : u32, id : String, name : String ) -> Result<ProtocolResources, WorkerError> {
    match p {
        Protocols::TMembership => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let sr = short_radio.expect("The TMembership protocol requires a short_radio to be provided.");
            //Initialize the radio.
            let listener = sr.init()?;
            let handler : Arc<Protocol> = Arc::new(TMembership::new(Arc::clone(&sr), seed, id, name));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, sr));
            let resources = ProtocolResources{  handler : handler, 
                                                radio_channels : radio_channels };
            Ok(resources)
        },

        Protocols::TMembershipAdvanced => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let sr = short_radio.expect("The TMembership_Advanced protocol requires a short_radio to be provided.");
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let lr = long_radio.expect("The TMembership_Advanced protocol requires a long_radio to be provided.");

            //Build the listeners list
            let mut radio_channels = Vec::new();
            let sr_listener = sr.init()?;
            let lr_listener = lr.init()?;
            radio_channels.push((sr_listener, Arc::clone(&sr)));
            radio_channels.push((lr_listener, Arc::clone(&lr)));

            //Build the protocol handler
            let handler : Arc<Protocol> = Arc::new(TMembershipAdvanced::new(sr, lr, seed, id, name));

            //Build the resources context
            let resources = ProtocolResources{ handler : handler,
                                               radio_channels : radio_channels };
            Ok(resources)
        },

        Protocols::NaiveRouting => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let sr = short_radio.expect("The NaiveRouting protocol requires a short_radio to be provided.");
            //Initialize the radio.
            let listener = sr.init()?;
            let handler : Arc<Protocol> = Arc::new(NaiveRouting::new(name, id, Arc::clone(&sr)));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, sr));
            let resources = ProtocolResources{  handler : handler, 
                                                radio_channels : radio_channels };
            Ok(resources)            
        }

    }
}