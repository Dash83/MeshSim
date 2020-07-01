//! Mesh simulator Protocol module.
//! This module includes the important Protocol trait that needs to be implement by
//! struct to be accepted as a protocol to be run by meshsim.

use crate::worker::listener::*;
use crate::worker::radio::*;
use crate::worker::{MessageHeader, Worker};
use crate::{MeshSimError, MeshSimErrorKind};
use aodv::AODV;
use gossip_routing::GossipRouting;
use lora_wifi_beacon::LoraWifiBeacon;
use naive_routing::NaiveRouting;
use reactive_gossip_routing::ReactiveGossipRouting;
use reactive_gossip_routing_II::ReactiveGossipRoutingII;
use reactive_gossip_routing_III::ReactiveGossipRoutingIII;
use slog::{Logger, KV};

use std::str::FromStr;
use std::sync::{Arc, Mutex};

pub mod aodv;
pub mod gossip_routing;
pub mod lora_wifi_beacon;
pub mod naive_routing;
pub mod reactive_gossip_routing;
#[allow(non_snake_case)]
pub mod reactive_gossip_routing_II;
#[allow(non_snake_case)]
pub mod reactive_gossip_routing_III;

type Outcome<'a> = (Option<MessageHeader>, Option<Box<dyn KV>>);

/// Trait that all protocols need to implement.
/// The function handle_message should
pub trait Protocol: std::fmt::Debug + Send + Sync {
    /// This function implements the state machine of the protocol. It will match against
    /// the message type passed and call the appropriate method to handle it.
    fn handle_message(
        &self,
        msg: MessageHeader,
        r_type: RadioTypes,
    ) -> Result<Outcome, MeshSimError>;

    /// Function to initialize the protocol.
    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError>;

    /// Function to send command to another node in the network
    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError>;
}

///Current list of supported protocols by MeshSim.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(tag = "Protocol")]
pub enum Protocols {
    /// Test protocol for Lora-Wifi integration
    LoraWifiBeacon,
    /// Flooding protocol
    NaiveRouting,
    /// Gossip-based flooding protocol
    GossipRouting {
        /// Minimum number of hops before applying probabilistic retransmission.
        k: usize,
        /// Probability of retransmission per node.
        p: f64,
    },
    ///  Adaptive, gossip-based routing protocol
    ReactiveGossip {
        /// Minimum number of hops before applying probabilistic retransmission.
        k: usize,
        /// Probability of retransmission per node.
        p: f64,
    },
    /// Improved version of RGR
    ReactiveGossipII {
        /// Minimum number of hops before applying probabilistic retransmission.
        k: usize,
        /// Base-probability of retransmission per node.
        p: f64,
        /// Adaptive-probability of retransmission per node.
        q: f64,
    },
    /// Improved version of RGRII that uses 2 radios
    ReactiveGossipIII {
        /// Minimum number of hops before applying probabilistic retransmission.
        k: usize,
        /// Probability of retransmission per node.
        p: f64,
        /// Adaptive-probability of retransmission per node.
        q: f64,
    },
    /// Ad-hoc On-Demand Distance Vector routing protocol.
    AODV,
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
                let (k, p) = {
                    let mut k = None;
                    let mut p = None;
                    for c in parts[1..].iter() {
                        let c: Vec<&str> = c.split('=').collect();
                        match c[0].to_uppercase().as_str() {
                            "K" => {
                                let x: usize = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid K value");
                                    MeshSimError {
                                        kind: MeshSimErrorKind::Configuration(err_msg),
                                        cause: Some(Box::new(e)),
                                    }
                                })?;
                                k = Some(x);
                            }
                            "P" => {
                                let x: f64 = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid P value");
                                    MeshSimError {
                                        kind: MeshSimErrorKind::Configuration(err_msg),
                                        cause: Some(Box::new(e)),
                                    }
                                })?;
                                p = Some(x);
                            }
                            _ => { /* Unrecognised option, do nothing */ }
                        }
                    }
                    if k.is_none() {
                        k = Some(reactive_gossip_routing::DEFAULT_MIN_HOPS);
                    }
                    if p.is_none() {
                        p = Some(reactive_gossip_routing::DEFAULT_GOSSIP_PROB);
                    }
                    (k.unwrap(), p.unwrap()) //Guaranted to not panic
                };
                Ok(Protocols::ReactiveGossip { k, p })
            }
            "REACTIVEGOSSIPII" => {
                // Expected format: ReactiveGossipII k=1, p=0.70
                let (k, p, q) = {
                    let mut k = None;
                    let mut p = None;
                    let mut q = None;
                    for c in parts[1..].iter() {
                        let c: Vec<&str> = c.split('=').collect();
                        match c[0].to_uppercase().as_str() {
                            "K" => {
                                let x: usize = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid K value");
                                    MeshSimError {
                                        kind: MeshSimErrorKind::Configuration(err_msg),
                                        cause: Some(Box::new(e)),
                                    }
                                })?;
                                k = Some(x);
                            }
                            "P" => {
                                let x: f64 = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid P value");
                                    MeshSimError {
                                        kind: MeshSimErrorKind::Configuration(err_msg),
                                        cause: Some(Box::new(e)),
                                    }
                                })?;
                                p = Some(x);
                            }
                            "Q" => {
                                let x: f64 = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid Q value");
                                    MeshSimError {
                                        kind: MeshSimErrorKind::Configuration(err_msg),
                                        cause: Some(Box::new(e)),
                                    }
                                })?;
                                q = Some(x);
                            }
                            _ => { /* Unrecognised option, do nothing */ }
                        }
                    }
                    if k.is_none() {
                        k = Some(reactive_gossip_routing_II::DEFAULT_MIN_HOPS);
                    }
                    if p.is_none() {
                        p = Some(reactive_gossip_routing_II::BASE_GOSSIP_PROB);
                    }
                    if q.is_none() {
                        q = Some(reactive_gossip_routing_II::VICINITY_GOSSIP_PROB);
                    }
                    (k.unwrap(), p.unwrap(), q.unwrap()) //Guaranted to not panic
                };
                Ok(Protocols::ReactiveGossipII { k, p, q })
            }
            "REACTIVEGOSSIPIII" => {
                // Expected format: ReactiveGossipIII k=1, p=0.70
                let (k, p, q) = {
                    let mut k = None;
                    let mut p = None;
                    let mut q = None;
                    for c in parts[1..].iter() {
                        let c: Vec<&str> = c.split('=').collect();

                        match c[0].to_uppercase().as_str() {
                            "K" => {
                                let x: usize = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid K value");
                                    MeshSimError {
                                        kind: MeshSimErrorKind::Configuration(err_msg),
                                        cause: Some(Box::new(e)),
                                    }
                                })?;
                                k = Some(x);
                            }
                            "P" => {
                                let x: f64 = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid P value");
                                    MeshSimError {
                                        kind: MeshSimErrorKind::Configuration(err_msg),
                                        cause: Some(Box::new(e)),
                                    }
                                })?;
                                p = Some(x);
                            }
                            "Q" => {
                                let x: f64 = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid Q value");
                                    MeshSimError {
                                        kind: MeshSimErrorKind::Configuration(err_msg),
                                        cause: Some(Box::new(e)),
                                    }
                                })?;
                                q = Some(x);
                            }
                            _ => { /* Unrecognised option, do nothing */ }
                        }
                    }
                    if k.is_none() {
                        k = Some(reactive_gossip_routing_III::DEFAULT_MIN_HOPS);
                    }
                    if p.is_none() {
                        p = Some(reactive_gossip_routing_III::BASE_GOSSIP_PROB);
                    }
                    if q.is_none() {
                        q = Some(reactive_gossip_routing_III::VICINITY_GOSSIP_PROB);
                    }
                    (k.unwrap(), p.unwrap(), q.unwrap()) //Guaranted to not panic
                };
                Ok(Protocols::ReactiveGossipIII { k, p, q })
            }
            "LORAWIFIBEACON" => Ok(Protocols::LoraWifiBeacon),
            "GOSSIPROUTING" => {
                // Expected format: GossipRouting k=1, p=0.70
                let (k, p) = {
                    let mut k = None;
                    let mut p = None;
                    for c in parts[1..].iter() {
                        let c: Vec<&str> = c.split('=').collect();

                        match c[0] {
                            "K" => {
                                let x: usize = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid K value");
                                    MeshSimError {
                                        kind: MeshSimErrorKind::Configuration(err_msg),
                                        cause: Some(Box::new(e)),
                                    }
                                })?;
                                k = Some(x);
                            }
                            "P" => {
                                let x: f64 = c[1].parse().map_err(|e| {
                                    let err_msg = String::from("Invalid P value");
                                    MeshSimError {
                                        kind: MeshSimErrorKind::Configuration(err_msg),
                                        cause: Some(Box::new(e)),
                                    }
                                })?;
                                p = Some(x);
                            }
                            _ => { /* Unrecognised option, do nothing */ }
                        }
                    }
                    if k.is_none() {
                        k = Some(gossip_routing::DEFAULT_MIN_HOPS);
                    }
                    if p.is_none() {
                        p = Some(gossip_routing::DEFAULT_GOSSIP_PROB);
                    }
                    (k.unwrap(), p.unwrap()) //Guaranted to not panic
                };
                Ok(Protocols::GossipRouting { k, p })
            }
            "AODV" => Ok(Protocols::AODV),
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
        Protocols::LoraWifiBeacon => {
            //Obtain the short-range radio.
            let (sr, sr_listener) = short_radio
                .expect("The LoraWifiBeacon protocol requires a Wifi radio to be provided.");
            //Obtain the long-range radio.
            let (lr, lr_listener) = long_radio
                .expect("The LoraWifiBeacon protocol requires a Lora radio to be provided.");

            //Build the listeners list
            let mut radio_channels = Vec::new();
            radio_channels.push((sr_listener, Arc::clone(&sr)));
            radio_channels.push((lr_listener, Arc::clone(&lr)));

            let rng = Worker::rng_from_seed(seed);

            //Build the protocol handler
            let handler: Arc<dyn Protocol> =
                Arc::new(LoraWifiBeacon::new(name, id, sr, lr, rng, logger));

            //Build the resources context
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }
        Protocols::NaiveRouting => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio
                .expect("The NaiveRouting protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(NaiveRouting::new(
                name,
                id,
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
        Protocols::GossipRouting { k, p } => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio
                .expect("The GossipRouting protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(GossipRouting::new(
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
        Protocols::ReactiveGossip { k, p } => {
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
        Protocols::ReactiveGossipII { k, p, q } => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio
                .expect("The ReactiveGossip protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(ReactiveGossipRoutingII::new(
                name,
                id,
                k,
                p,
                q,
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
        Protocols::ReactiveGossipIII { k, p, q } => {
            //Obtain both radios
            let (sr, sr_listener) = short_radio
                .expect("The ReactiveGossip protocol requires a short_radio to be provided.");
            let (lr, lr_listener) = long_radio
                .expect("The LoraWifiBeacon protocol requires a Lora radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(ReactiveGossipRoutingIII::new(
                name,
                id,
                k,
                p,
                q,
                Arc::clone(&sr),
                Arc::clone(&lr),
                Arc::new(Mutex::new(rng)),
                logger,
            ));
            let mut radio_channels = Vec::new();
            radio_channels.push((sr_listener, sr));
            radio_channels.push((lr_listener, lr));
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }
        Protocols::AODV => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) =
                short_radio.expect("The AODV protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(AODV::new(
                name,
                id,
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
    }
}
