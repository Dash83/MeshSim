//! Mesh simulator Protocol module.
//! This module includes the important Protocol trait that needs to be implement by
//! struct to be accepted as a protocol to be run by meshsim.

use crate::worker::listener::*;
use crate::worker::radio::*;
use crate::worker::{MessageHeader, Worker};
use crate::{MeshSimError, MeshSimErrorKind};
use lora_wifi_beacon::LoraWifiBeacon;
use naive_routing::NaiveRouting;
use reactive_gossip_routing::ReactiveGossipRouting;
use reactive_gossip_routing_II::ReactiveGossipRoutingII;
use gossip_routing::GossipRouting;
use slog::Logger;

use std;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub mod lora_wifi_beacon;
pub mod naive_routing;
pub mod reactive_gossip_routing;
#[allow(non_snake_case)]
pub mod reactive_gossip_routing_II;
pub mod gossip_routing;

/// Trait that all protocols need to implement.
/// The function handle_message should
pub trait Protocol: std::fmt::Debug + Send + Sync {
    /// This function implements the state machine of the protocol. It will match against
    /// the message type passed and call the appropriate method to handle it.
    fn handle_message(
        &self,
        msg: MessageHeader,
        r_type: RadioTypes,
    ) -> Result<Option<MessageHeader>, MeshSimError>;

    /// Function to initialize the protocol.
    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError>;

    /// Function to send command to another node in the network
    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError>;
}

///Current list of supported protocols by MeshSim.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(tag = "Protocol")]
pub enum Protocols {
    /// Broadcast-based routing
    NaiveRouting,
    ///  Adaptive, gossip-based routing protocol
    ReactiveGossip{
        /// Minimum number of hops before applying probabilistic retransmission.
        k: usize,
        /// Probability of retransmission per node.
        p: f64
    },
    /// Test protocol for Lora-Wifi integration
    LoraWifiBeacon,
    /// Improved version of RGR
    ReactiveGossipII{
        /// Minimum number of hops before applying probabilistic retransmission.
        k: usize,
        /// Probability of retransmission per node.
        p: f64
    },
    /// Gossip-based flooding protocol
    GossipRouting{
        /// Minimum number of hops before applying probabilistic retransmission.
        k: usize,
        /// Probability of retransmission per node.
        p: f64
    },
}

impl Default for Protocols {
    fn default() -> Self {
        Protocols::NaiveRouting
    }
}

//The FromStr implementation is used for the TestGen utility only. For other convertions, we use the
//serialization/deserialization that serde provides.
impl FromStr for Protocols {
    type Err = MeshSimError;

    fn from_str(s: &str) -> Result<Protocols, MeshSimError> {
        let input = s.to_uppercase();
        let parts: Vec<&str> = input.split_whitespace().collect();

        assert!(!parts.is_empty());
        let prot = parts[0];
        match prot {
            "NAIVEROUTING" => Ok(Protocols::NaiveRouting),
            "REACTIVEGOSSIP" => {
                // Expected format: ReactiveGossip k=1, p=0.70
                let (k,p) = {
                    let mut k = None;
                    let mut p = None;
                    for c in parts[1..].iter() {
                        let c: Vec<&str> = c.split('=').collect();

                        match c[0] {
                            "K" => {
                                let x: usize = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid K value");
                                    MeshSimError{
                                        kind : MeshSimErrorKind::Configuration(err_msg),
                                        cause : Some(Box::new(e))
                                    }
                                })?;
                                k = Some(x);
                            },
                            "P" => {
                                let x: f64 = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid P value");
                                    MeshSimError{
                                        kind : MeshSimErrorKind::Configuration(err_msg),
                                        cause : Some(Box::new(e))
                                    }
                                })?;
                                p = Some(x);
                            },
                            _ => {
                                /* Unrecognised option, do nothing */
                            },
                        }
                    }
                    if k.is_none() {
                        k = Some(reactive_gossip_routing::DEFAULT_MIN_HOPS);
                    }
                    if p.is_none() {
                        p = Some(reactive_gossip_routing::DEFAULT_GOSSIP_PROB);
                    }
                    (k.unwrap(), p.unwrap())
                };
                Ok(
                    Protocols::ReactiveGossip{k, p}
                )
            },
            "REACTIVEGOSSIPII" => {
                // Expected format: ReactiveGossipII k=1, p=0.70
                let (k,p) = {
                    let mut k = None;
                    let mut p = None;
                    for c in parts[1..].iter() {
                        let c: Vec<&str> = c.split('=').collect();

                        match c[0] {
                            "K" => {
                                let x: usize = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid K value");
                                    MeshSimError{
                                        kind : MeshSimErrorKind::Configuration(err_msg),
                                        cause : Some(Box::new(e))
                                    }
                                })?;
                                k = Some(x);
                            },
                            "P" => {
                                let x: f64 = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid P value");
                                    MeshSimError{
                                        kind : MeshSimErrorKind::Configuration(err_msg),
                                        cause : Some(Box::new(e))
                                    }
                                })?;
                                p = Some(x);
                            },
                            _ => {
                                /* Unrecognised option, do nothing */
                            },
                        }
                    }
                    if k.is_none() {
                        k = Some(reactive_gossip_routing_II::DEFAULT_MIN_HOPS);
                    }
                    if p.is_none() {
                        p = Some(reactive_gossip_routing_II::BASE_GOSSIP_PROB);
                    }
                    (k.unwrap(), p.unwrap())
                };
                Ok(
                    Protocols::ReactiveGossipII{k, p}
                )
            },
            "LORAWIFIBEACON" => Ok(Protocols::LoraWifiBeacon),
            "GOSSIPROUTING" => {
                // Expected format: GossipRouting k=1, p=0.70
                let (k,p) = {
                    let mut k = None;
                    let mut p = None;
                    for c in parts[1..].iter() {
                        let c: Vec<&str> = c.split('=').collect();

                        match c[0] {
                            "K" => {
                                let x: usize = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid K value");
                                    MeshSimError{
                                        kind : MeshSimErrorKind::Configuration(err_msg),
                                        cause : Some(Box::new(e))
                                    }
                                })?;
                                k = Some(x);
                            },
                            "P" => {
                                let x: f64 = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid P value");
                                    MeshSimError{
                                        kind : MeshSimErrorKind::Configuration(err_msg),
                                        cause : Some(Box::new(e))
                                    }
                                })?;
                                p = Some(x);
                            },
                            _ => {
                                /* Unrecognised option, do nothing */
                            },
                        }
                    }
                    if k.is_none() {
                        k = Some(gossip_routing::DEFAULT_MIN_HOPS);
                    }
                    if p.is_none() {
                        p = Some(gossip_routing::DEFAULT_GOSSIP_PROB);
                    }
                    (k.unwrap(), p.unwrap())
                };
                Ok(
                    Protocols::GossipRouting{k, p}
                )
            },
            _ => {
                let err_msg = String::from("The specified protocol is not supported.");
                let error = MeshSimError {
                    kind: MeshSimErrorKind::Configuration(err_msg),
                    cause: None,
                };
                Err(error)
            }
        }
    }
}

/// Helper struct for the return type of build_protocol_resources().
/// It contains all the protocol-releated resources necessary for the worker to do it's job.
pub struct ProtocolResources {
    ///Protocol handler for the selected protocol.
    pub handler: Arc<dyn Protocol>,
    ///Collection of rx/tx radio channels for the worker and protocol to communicate.
    pub radio_channels: Vec<(Box<dyn Listener>, Arc<dyn Radio>)>,
}

/// Provides a new boxed reference to the struct matching the passed protocol.
pub fn build_protocol_resources(
    p: Protocols,
    short_radio: Option<(Arc<dyn Radio>, Box<dyn Listener>)>,
    long_radio: Option<(Arc<dyn Radio>, Box<dyn Listener>)>,
    seed: u32,
    id: String,
    name: String,
    logger: Logger,
) -> Result<ProtocolResources, MeshSimError> {
    match p {
        Protocols::NaiveRouting => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio
                .expect("The NaiveRouting protocol requires a short_radio to be provided.");
            let handler: Arc<dyn Protocol> =
                Arc::new(NaiveRouting::new(name, id, Arc::clone(&sr), logger));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, sr));
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }

        Protocols::ReactiveGossip{k, p} => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio
                .expect("The ReactiveGossip protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(ReactiveGossipRouting::new(
                name,
                id,
                k,
                p,
                Arc::clone(&sr),
                Arc::new(Mutex::new(rng)),
                logger,
            ));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, sr));
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }

        Protocols::ReactiveGossipII{k, p} => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio
                .expect("The ReactiveGossip protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(ReactiveGossipRoutingII::new(
                name,
                id,
                k,
                p,
                Arc::clone(&sr),
                Arc::new(Mutex::new(rng)),
                logger,
            ));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, sr));
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }

        Protocols::LoraWifiBeacon => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, sr_listener) = short_radio
                .expect("The LoraWifiBeacon protocol requires a Wifi radio to be provided.");
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (lr, lr_listener) = long_radio
                .expect("The LoraWifiBeacon protocol requires a Lora radio to be provided.");

            //Build the listeners list
            let mut radio_channels = Vec::new();
            radio_channels.push((sr_listener, Arc::clone(&sr)));
            radio_channels.push((lr_listener, Arc::clone(&lr)));

            let _rng = Worker::rng_from_seed(seed);

            //Build the protocol handler
            let handler: Arc<dyn Protocol> =
                Arc::new(LoraWifiBeacon::new(name, id, sr, lr, logger));

            //Build the resources context
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }

        Protocols::GossipRouting{k, p} => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio
                .expect("The GossipRouting protocol requires a short_radio to be provided.");
            let handler: Arc<dyn Protocol> =
                Arc::new(GossipRouting::new(
                    name,
                    id,
                    k,
                    p,
                    Arc::clone(&sr),
                    logger));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, sr));
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }
    }
}
