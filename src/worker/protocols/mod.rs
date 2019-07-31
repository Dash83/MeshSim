//! Mesh simulator Protocol module.
//! This module includes the important Protocol trait that needs to be implement by
//! struct to be accepted as a protocol to be run by meshsim.

extern crate rand;
extern crate toml;
extern crate rustc_serialize;

use worker::{MessageHeader, WorkerError, Worker };
use worker::radio::*;
use worker::listener::*;
use self::tmembership::TMembership;
use self::tmembership_advanced::TMembershipAdvanced;
use self::naive_routing::NaiveRouting;
use self::reactive_gossip_routing::ReactiveGossipRouting;
use self::reactive_gossip_routing_II::ReactiveGossipRoutingII;
use self::lora_wifi_beacon::LoraWifiBeacon;
use ::slog::Logger;

use std;
use std::sync::Arc;
use std::str::FromStr;

pub mod tmembership;
pub mod tmembership_advanced;
pub mod naive_routing;
pub mod reactive_gossip_routing;
pub mod lora_wifi_beacon;
pub mod reactive_gossip_routing_II;

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
    /// Broadcast-based routing
    NaiveRouting,
    ///  Adaptive, gossip-based routing protocol
    ReactiveGossip,
    /// Test protocol for Lora-Wifi integration
    LoraWifiBeacon,
    /// Improved version of RGR
    ReactiveGossipII,
}

impl FromStr for Protocols {
    type Err = WorkerError;

    fn from_str(s : &str) -> Result<Protocols, WorkerError> {
        let input = s.to_uppercase();
        let parts : Vec<&str> = input.split_whitespace().collect();

        assert!(parts.len() > 0);
        let prot = parts[0];
        match prot {
            "TMEMBERSHIP" => Ok(Protocols::TMembership),
            "TMEMBERSHIP_ADVANCED" => Ok(Protocols::TMembershipAdvanced),
            "NAIVEROUTING" => Ok(Protocols::NaiveRouting),
            "REACTIVEGOSSIP" => Ok(Protocols::ReactiveGossip),
            "LORAWIFIBEACON" => Ok(Protocols::LoraWifiBeacon),
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
                                 short_radio : Option<(Arc<Radio>, Box<Listener>)>,
                                 long_radio  : Option<(Arc<Radio>, Box<Listener>)>,
                                 seed : u32, 
                                 id : String, 
                                 name : String,
                                 logger : Logger ) -> Result<ProtocolResources, WorkerError> {
    match p {
        Protocols::TMembership => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio.expect("The TMembership protocol requires a short_radio to be provided.");
            let handler : Arc<Protocol> = Arc::new(TMembership::new(Arc::clone(&sr), seed, id, name, logger));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, sr));
            let resources = ProtocolResources{  handler : handler, 
                                                radio_channels : radio_channels };
            Ok(resources)
        },

        Protocols::TMembershipAdvanced => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, sr_listener) = short_radio.expect("The TMembership_Advanced protocol requires a short_radio to be provided.");
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (lr, lr_listener) = long_radio.expect("The TMembership_Advanced protocol requires a long_radio to be provided.");

            //Build the listeners list
            let mut radio_channels = Vec::new();
            radio_channels.push((sr_listener, Arc::clone(&sr)));
            radio_channels.push((lr_listener, Arc::clone(&lr)));

            //Build the protocol handler
            let handler : Arc<Protocol> = Arc::new(TMembershipAdvanced::new(sr, lr, seed, id, name, logger));

            //Build the resources context
            let resources = ProtocolResources{ handler : handler,
                                               radio_channels : radio_channels };
            Ok(resources)
        },

        Protocols::NaiveRouting => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio.expect("The NaiveRouting protocol requires a short_radio to be provided.");
            let handler : Arc<Protocol> = Arc::new(NaiveRouting::new(name, id, Arc::clone(&sr), logger));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, sr));
            let resources = ProtocolResources{  handler : handler, 
                                                radio_channels : radio_channels };
            Ok(resources)            
        },

        Protocols::ReactiveGossip => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio.expect("The ReactiveGossip protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler : Arc<Protocol> = Arc::new(ReactiveGossipRouting::new(name, id, Arc::clone(&sr), rng, logger));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, sr));
            let resources = ProtocolResources{  handler : handler, 
                                                radio_channels : radio_channels };
            Ok(resources)            
        },

        Protocols::ReactiveGossipII => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio.expect("The ReactiveGossip protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler : Arc<Protocol> = Arc::new(ReactiveGossipRoutingII::new(name, id, Arc::clone(&sr), rng, logger));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, sr));
            let resources = ProtocolResources{  handler : handler,
                radio_channels : radio_channels };
            Ok(resources)
        },

        Protocols::LoraWifiBeacon => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, sr_listener) = short_radio.expect("The LoraWifiBeacon protocol requires a Wifi radio to be provided.");
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (lr, lr_listener) = long_radio.expect("The LoraWifiBeacon protocol requires a Lora radio to be provided.");

            //Build the listeners list
            let mut radio_channels = Vec::new();
            radio_channels.push((sr_listener, Arc::clone(&sr)));
            radio_channels.push((lr_listener, Arc::clone(&lr)));

            let rng = Worker::rng_from_seed(seed);

            //Build the protocol handler
            let handler : Arc<Protocol> = Arc::new(LoraWifiBeacon::new(name, id, sr, lr,  logger));

            //Build the resources context
            let resources = ProtocolResources{ handler : handler,
                                               radio_channels : radio_channels };
            Ok(resources)
        },

    }
}