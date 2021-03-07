//! Mesh simulator Protocol module.
//! This module includes the important Protocol trait that needs to be implement by
//! struct to be accepted as a protocol to be run by meshsim.

use crate::worker::listener::*;
use crate::worker::radio::*;
use crate::worker::{MessageHeader, Worker};
use crate::{MeshSimError, MeshSimErrorKind};
use aodv::AODV;
use chrono::{DateTime, Utc};
use gossip_routing::GossipRouting;
use lora_wifi_beacon::LoraWifiBeacon;
use flooding::Flooding;
use reactive_gossip_routing::ReactiveGossipRouting;
use reactive_gossip_routing_II::ReactiveGossipRoutingII;
use reactive_gossip_routing_III::ReactiveGossipRoutingIII;
use slog::{Logger, Record, Serializer, KV};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use crossbeam_channel::{unbounded, Receiver, Sender};

pub mod aodv;
pub mod gossip_routing;
pub mod lora_wifi_beacon;
pub mod flooding;
pub mod reactive_gossip_routing;
#[allow(non_snake_case)]
pub mod reactive_gossip_routing_II;
#[allow(non_snake_case)]
pub mod reactive_gossip_routing_III;

type Transmission = (MessageHeader, ProtocolMessages, DateTime<Utc>);
type HandleMessageOutcome = Option<(MessageHeader, ProtocolMessages)>;
/// Trait that all protocols need to implement.
/// The function handle_message should
pub trait Protocol: std::fmt::Debug + Send + Sync {
    /// This function implements the state machine of the protocol. It will match against
    /// the message type passed and call the appropriate method to handle it.
    fn handle_message(
        &self,
        msg: MessageHeader,
        ts: DateTime<Utc>,
        r_type: RadioTypes,
    ) -> Result<(), MeshSimError>;

    /// Function to initialize the protocol.
    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError>;

    /// Function to send command to another node in the network
    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError>;

    /// Function called to trigger maintenance operations the protoco might need, such as
    // route maintenance or packet retransmission.
    fn do_maintenance(&self) -> Result<(), MeshSimError>;
}

///Current list of supported protocols by MeshSim.
#[derive(Debug, Deserialize, Serialize, PartialEq, Clone, Copy)]
#[serde(tag = "Protocol")]
pub enum Protocols {
    /// Test protocol for Lora-Wifi integration
    LoraWifiBeacon,
    /// Flooding protocol
    Flooding,
    /// Gossip-based flooding protocol
    Gossip {
        /// Minimum number of hops before applying probabilistic retransmission.
        k: usize,
        /// Probability of retransmission per node.
        p: f64,
    },
    ///  Adaptive, gossip-based routing protocol
    RGR {
        /// Minimum number of hops before applying probabilistic retransmission.
        k: Option<usize>,
        /// Probability of retransmission per node.
        p: Option<f64>,
    },
    /// Improved version of RGR
    RGRII {
        /// Minimum number of hops before applying probabilistic retransmission.
        k: Option<usize>,
        /// Base-probability of retransmission per node.
        p: Option<f64>,
        /// Adaptive-probability of retransmission per node.
        q: Option<f64>,
    },
    /// Improved version of RGRII that uses 2 radios
    RGRIII {
        /// Minimum number of hops before applying probabilistic retransmission.
        k: Option<usize>,
        /// Base-probability of retransmission per node.
        p: Option<f64>,
        /// Adaptive-probability of retransmission per node.
        q: Option<f64>,
        /// Periodicity for sending beacon messages over the long-radio. In milliseconds.
        beacon_threshold: Option<i64>,
    },
    /// Ad-hoc On-Demand Distance Vector routing protocol.
    AODV {
        active_route_timeout: Option<i64>,
        net_diameter: Option<usize>,
        node_traversal_time: Option<i64>,
        allowed_hello_loss: Option<usize>,
        rreq_retries: Option<usize>,
        next_hop_wait: Option<i64>,
    },
}

impl Default for Protocols {
    fn default() -> Self {
        Protocols::Flooding
    }
}

impl Protocols {
    pub fn get_data_analysis_patterns(&self) -> HashMap<String, String> {
        match *self {
            Protocols::LoraWifiBeacon => { HashMap::new() },
            Protocols::Flooding => { 
                let mut data = HashMap::new();
                data.insert("OK_PKT_PATTERN".into(), "ACCEPTED.*DATA".into());
                data.insert("DATA_PATTERN".into(), "SENT.*DATA".into());
                data.insert("RD_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("RE_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("RT_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("MAINTENANCE_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("ROUTE_BREAK_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("RD_FAIL_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data
            },
            Protocols::Gossip{p: _, k: _} => { 
                let mut data = HashMap::new();
                data.insert("OK_PKT_PATTERN".into(), "ACCEPTED.*DATA".into());
                data.insert("DATA_PATTERN".into(), "SENT.*DATA".into());
                data.insert("RD_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("RE_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("RT_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("MAINTENANCE_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("ROUTE_BREAK_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("RD_FAIL_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data
            },
            Protocols::AODV { 
                active_route_timeout: _, 
                net_diameter: _, 
                node_traversal_time: _, 
                allowed_hello_loss: _, 
                rreq_retries: _, 
                next_hop_wait: _ } => {
                    let mut data = HashMap::new();
                    data.insert("OK_PKT_PATTERN".into(), "ACCEPTED.*DATA".into());
                    data.insert("DATA_PATTERN".into(), "SENT.*DATA".into());
                    data.insert("RD_PATTERN".into(), "SENT.*RREQ".into());
                    data.insert("RE_PATTERN".into(), "SENT.*RREP".into());
                    data.insert("RT_PATTERN".into(), "SENT.*RERR".into());
                    data.insert("MAINTENANCE_PATTERN".into(), "SENT.*HELLO".into());
                    data.insert("ROUTE_BREAK_PATTERN".into(), "BROKEN_LINK detected".into());
                    data.insert("RD_FAIL_PATTERN".into(), "RREQ retries exceeded".into());
                    data                    
            }
            Protocols::RGR{p: _, k: _} => { 
                let mut data = HashMap::new();
                data.insert("OK_PKT_PATTERN".into(), "ACCEPTED.*DATA".into());
                data.insert("DATA_PATTERN".into(), "SENT.*DATA".into());
                data.insert("RD_PATTERN".into(), "SENT.*ROUTE_DISCOVERY".into());
                data.insert("RE_PATTERN".into(), "SENT.*ROUTE_ESTABLISH".into());
                data.insert("RT_PATTERN".into(), "SENT.*ROUTE_TEARDOWN".into());
                data.insert("MAINTENANCE_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("ROUTE_BREAK_PATTERN".into(), "Route Teardown initiated".into());
                data.insert("RD_FAIL_PATTERN".into(), "ROUTE_DISCOVERY retries exceeded".into());
                data
            },
            Protocols::RGRII{p: _, k: _, q: _} => { 
                let mut data = HashMap::new();
                data.insert("OK_PKT_PATTERN".into(), "ACCEPTED.*DATA".into());
                data.insert("DATA_PATTERN".into(), "SENT.*DATA".into());
                data.insert("RD_PATTERN".into(), "SENT.*ROUTE_DISCOVERY".into());
                data.insert("RE_PATTERN".into(), "SENT.*ROUTE_ESTABLISH".into());
                data.insert("RT_PATTERN".into(), "SENT.*ROUTE_TEARDOWN".into());
                data.insert("MAINTENANCE_PATTERN".into(), "SOMETHINGTHATWILLNEVERMATCH".into());
                data.insert("ROUTE_BREAK_PATTERN".into(), "Route Teardown initiated".into());
                data.insert("RD_FAIL_PATTERN".into(), "ROUTE_DISCOVERY retries exceeded".into());
                data
            },
            Protocols::RGRIII{p: _, k: _, q: _, beacon_threshold: _} => { 
                let mut data = HashMap::new();
                data.insert("OK_PKT_PATTERN".into(), "ACCEPTED.*DATA".into());
                data.insert("DATA_PATTERN".into(), "SENT.*DATA".into());
                data.insert("RD_PATTERN".into(), "SENT.*ROUTE_DISCOVERY".into());
                data.insert("RE_PATTERN".into(), "SENT.*ROUTE_ESTABLISH".into());
                data.insert("RT_PATTERN".into(), "SENT.*ROUTE_TEARDOWN".into());
                data.insert("MAINTENANCE_PATTERN".into(), "SENT.*BEACON".into());
                data.insert("ROUTE_BREAK_PATTERN".into(), "Route Teardown initiated".into());
                data.insert("RD_FAIL_PATTERN".into(), "ROUTE_DISCOVERY retries exceeded".into());
                data
            },
        }
    }
}

pub enum ProtocolMessages {
    AODV(aodv::Messages),
    Gossip(gossip_routing::Messages),
    LoraWifi(lora_wifi_beacon::Messages),
    Flooding(flooding::Messages),
    RGRI(reactive_gossip_routing::Messages),
    RGRII(reactive_gossip_routing_II::Messages),
    RGRIII(reactive_gossip_routing_III::Messages),
}

impl KV for ProtocolMessages {
    fn serialize(&self, _rec: &Record, serializer: &mut dyn Serializer) -> slog::Result {
        match *self {
            ProtocolMessages::AODV(ref msg) => match msg {
                aodv::Messages::DATA(ref m) => {
                    let _ = serializer.emit_str("msg_type", "DATA")?;
                    serializer.emit_str("msg_destination", &m.destination)
                }
                aodv::Messages::RREQ(ref m) => {
                    let _ = serializer.emit_str("msg_type", "RREQ")?;
                    let _ = serializer.emit_str("msg_source", &m.originator)?;
                    let _ = serializer.emit_str("msg_destination", &m.destination)?;
                    serializer.emit_u32("rreq_id", m.rreq_id)
                }
                aodv::Messages::RREP(ref m) => {
                    let _ = serializer.emit_str("msg_type", "RREP")?;
                    let _ = serializer.emit_str("msg.originator", &m.originator)?;
                    let _ = serializer.emit_str("msg.destination", &m.destination)?;
                    serializer.emit_u32("dest_seq_no", m.dest_seq_no)
                }
                aodv::Messages::RERR(ref m) => {
                    let _ = serializer.emit_str("msg_type", "RERR")?;
                    serializer.emit_usize("msg.num_affected_destinations", m.destinations.len())
                }
                aodv::Messages::HELLO(ref _m) => serializer.emit_str("msg_type", "HELLO"),
                aodv::Messages::RREP_ACK => serializer.emit_str("msg_type", "RREP_ACK"),
            },
            ProtocolMessages::Gossip(ref _msg) => serializer.emit_str("msg_type", "DATA"),
            ProtocolMessages::Flooding(ref _msg) => serializer.emit_str("msg_type", "DATA"),
            ProtocolMessages::LoraWifi(ref msg) => match msg {
                lora_wifi_beacon::Messages::Beacon(m) => {
                    let _ = serializer.emit_str("msg_type", "BEACON")?;
                    serializer.emit_u64("id", m.0)
                }
                lora_wifi_beacon::Messages::BeaconResponse(m) => {
                    let _ = serializer.emit_str("msg_type", "BEACON_RESPONSE")?;
                    serializer.emit_u64("id", m.0)
                }
            },
            ProtocolMessages::RGRI(ref msg) => match msg {
                reactive_gossip_routing::Messages::Data(ref m) => {
                    let _ = serializer.emit_str("msg_type", "DATA")?;
                    serializer.emit_str("route_id", &m.route_id)
                }
                reactive_gossip_routing::Messages::RouteDiscovery(ref m) => {
                    let _ = serializer.emit_str("msg_type", "ROUTE_DISCOVERY")?;
                    let _ = serializer.emit_str("route_id", &m.route_id)?;
                    let _ = serializer.emit_str("msg_source", &m.route_source)?;
                    let _ = serializer.emit_str("msg_destination", &m.route_destination)?;
                    serializer.emit_usize("route_length", m.route.len())
                }
                reactive_gossip_routing::Messages::RouteEstablish(ref m) => {
                    let _ = serializer.emit_str("msg_type", "ROUTE_ESTABLISH")?;
                    let _ = serializer.emit_str("route_id", &m.route_id)?;
                    let _ = serializer.emit_str("msg_source", &m.route_source)?;
                    let _ = serializer.emit_str("msg_destination", &m.route_destination)?;
                    serializer.emit_usize("route_length", m.route.len())
                }
                reactive_gossip_routing::Messages::RouteTeardown(ref m) => {
                    let _ = serializer.emit_str("msg_type", "ROUTE_TEARDOWN")?;
                    serializer.emit_str("route_id", &m.route_id)
                }
            },
            ProtocolMessages::RGRII(ref msg) => match msg {
                reactive_gossip_routing_II::Messages::Data(ref m) => {
                    let _ = serializer.emit_str("msg_type", "DATA")?;
                    serializer.emit_str("route_id", &m.route_id)
                }
                reactive_gossip_routing_II::Messages::RouteDiscovery(ref m) => {
                    let _ = serializer.emit_str("msg_type", "ROUTE_DISCOVERY")?;
                    let _ = serializer.emit_str("route_id", &m.route_id)?;
                    let _ = serializer.emit_str("msg_source", &m.route_source)?;
                    let _ = serializer.emit_str("msg_destination", &m.route_destination)?;
                    serializer.emit_usize("route_length", m.route.len())
                }
                reactive_gossip_routing_II::Messages::RouteEstablish(ref m) => {
                    let _ = serializer.emit_str("msg_type", "ROUTE_ESTABLISH")?;
                    let _ = serializer.emit_str("route_id", &m.route_id)?;
                    let _ = serializer.emit_str("msg_source", &m.route_source)?;
                    let _ = serializer.emit_str("msg_destination", &m.route_destination)?;
                    serializer.emit_usize("route_length", m.route.len())
                }
                reactive_gossip_routing_II::Messages::RouteTeardown(ref m) => {
                    let _ = serializer.emit_str("msg_type", "ROUTE_TEARDOWN")?;
                    serializer.emit_str("route_id", &m.route_id)
                }
            },
            ProtocolMessages::RGRIII(ref msg) => match msg {
                reactive_gossip_routing_III::Messages::Data(ref m) => {
                    let _ = serializer.emit_str("msg_type", "DATA")?;
                    serializer.emit_str("route_id", &m.route_id)
                }
                reactive_gossip_routing_III::Messages::RouteDiscovery(ref m) => {
                    let _ = serializer.emit_str("msg_type", "ROUTE_DISCOVERY")?;
                    let _ = serializer.emit_str("route_id", &m.route_id)?;
                    let _ = serializer.emit_str("msg_source", &m.route_source)?;
                    let _ = serializer.emit_str("msg_destination", &m.route_destination)?;
                    serializer.emit_usize("route_length", m.route.len())
                }
                reactive_gossip_routing_III::Messages::RouteEstablish(ref m) => {
                    let _ = serializer.emit_str("msg_type", "ROUTE_ESTABLISH")?;
                    let _ = serializer.emit_str("route_id", &m.route_id)?;
                    let _ = serializer.emit_str("msg_source", &m.route_source)?;
                    let _ = serializer.emit_str("msg_destination", &m.route_destination)?;
                    serializer.emit_usize("route_length", m.route.len())
                }
                reactive_gossip_routing_III::Messages::RouteTeardown(ref m) => {
                    let _ = serializer.emit_str("msg_type", "ROUTE_TEARDOWN")?;
                    serializer.emit_str("route_id", &m.route_id)
                }
                reactive_gossip_routing_III::Messages::Beacon(ref _m) => {
                    serializer.emit_str("msg_type", "BEACON")
                }
            },
        }
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
            "FLOODING" => Ok(Protocols::Flooding),
            "RGR" => {
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

                Ok(Protocols::RGR { k, p })
            }
            "RGRII" => {
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

                Ok(Protocols::RGRII { k, p, q })
            }
            "RGRIII" => {
                let mut k = None;
                let mut p = None;
                let mut q = None;
                let mut beacon_threshold = None;
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
                        },
                        "P" => {
                            let x: f64 = c[1].parse().map_err(|e| {
                                let err_msg = String::from("Invalid P value");
                                MeshSimError {
                                    kind: MeshSimErrorKind::Configuration(err_msg),
                                    cause: Some(Box::new(e)),
                                }
                            })?;
                            p = Some(x);
                        },
                        "Q" => {
                            let x: f64 = c[1].parse().map_err(|e| {
                                let err_msg = String::from("Invalid Q value");
                                MeshSimError {
                                    kind: MeshSimErrorKind::Configuration(err_msg),
                                    cause: Some(Box::new(e)),
                                }
                            })?;
                            q = Some(x);
                        },
                        "BEACON_THRESHOLD" => {
                            let x: i64 = c[1].parse().map_err(|e| {
                                let err_msg = String::from("Invalid beacon_threshold value");
                                MeshSimError {
                                    kind: MeshSimErrorKind::Configuration(err_msg),
                                    cause: Some(Box::new(e)),
                                }
                            })?;
                            beacon_threshold = Some(x);
                        }
                        _ => { /* Unrecognised option, do nothing */ }
                    }
                }

                Ok(Protocols::RGRIII { k, p, q, beacon_threshold })
            }
            "LORAWIFIBEACON" => Ok(Protocols::LoraWifiBeacon),
            "GOSSIP" => {
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
                Ok(Protocols::Gossip { k, p })
            }
            "AODV" => {
                let mut active_route_timeout: Option<i64> = Default::default();
                let mut net_diameter: Option<usize> = Default::default();
                let mut node_traversal_time: Option<i64> = Default::default();
                let mut allowed_hello_loss: Option<usize> = Default::default();
                let mut rreq_retries: Option<usize> = Default::default();
                let mut next_hop_wait: Option<i64> = Default::default();

                for c in parts[1..].iter() {
                    let c: Vec<&str> = c.split('=').collect();
                    match c[0].to_uppercase().as_str() {
                        "active_route_timeout" => {
                            let x: i64 = c[1].parse().map_err(|e| {
                                let err_msg = String::from("Invalid active_route_timeout value");
                                MeshSimError {
                                    kind: MeshSimErrorKind::Configuration(err_msg),
                                    cause: Some(Box::new(e)),
                                }
                            })?;
                            active_route_timeout = Some(x);
                        },
                        "net_diameter" => {
                            let x: usize = c[1].parse().map_err(|e| {
                                let err_msg = String::from("Invalid net_diameter value");
                                MeshSimError {
                                    kind: MeshSimErrorKind::Configuration(err_msg),
                                    cause: Some(Box::new(e)),
                                }
                            })?;
                            net_diameter = Some(x);
                        },
                        "node_traversal_time" => {
                            let x: i64 = c[1].parse().map_err(|e| {
                                let err_msg = String::from("Invalid node_traversal_time value");
                                MeshSimError {
                                    kind: MeshSimErrorKind::Configuration(err_msg),
                                    cause: Some(Box::new(e)),
                                }
                            })?;
                            node_traversal_time = Some(x);
                        },
                        "allowed_hello_loss" => {
                            let x: usize = c[1].parse().map_err(|e| {
                                let err_msg = String::from("Invalid allowed_hello_loss value");
                                MeshSimError {
                                    kind: MeshSimErrorKind::Configuration(err_msg),
                                    cause: Some(Box::new(e)),
                                }
                            })?;
                            allowed_hello_loss = Some(x);
                        },
                        "rreq_retries" => {
                            let x: usize = c[1].parse().map_err(|e| {
                                let err_msg = String::from("Invalid rreq_retries value");
                                MeshSimError {
                                    kind: MeshSimErrorKind::Configuration(err_msg),
                                    cause: Some(Box::new(e)),
                                }
                            })?;
                            rreq_retries = Some(x);
                        },
                        "next_hop_wait" => {
                            let x: i64 = c[1].parse().map_err(|e| {
                                let err_msg = String::from("Invalid next_hop_wait value");
                                MeshSimError {
                                    kind: MeshSimErrorKind::Configuration(err_msg),
                                    cause: Some(Box::new(e)),
                                }
                            })?;
                            next_hop_wait = Some(x);
                        }
                        _ => { /* Unrecognised option, do nothing */ }
                    }
                }

                Ok(Protocols::AODV {
                    active_route_timeout,
                    net_diameter,
                    node_traversal_time,
                    allowed_hello_loss,
                    rreq_retries,
                    next_hop_wait,
                })
            }
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
    pub radio_channels: Vec<(Box<dyn Listener>, (Arc<dyn Radio>, Sender<Transmission>, Receiver<Transmission>))>,
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
    let (sr_sender, sr_receiver) = unbounded();
    let (lr_sender, lr_receiver) = unbounded();
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
            radio_channels.push((sr_listener, (Arc::clone(&sr), sr_sender.clone(), sr_receiver)));
            radio_channels.push((lr_listener, (Arc::clone(&lr), lr_sender.clone(), lr_receiver)));

            let rng = Worker::rng_from_seed(seed);

            //Build the protocol handler
            let handler: Arc<dyn Protocol> =
                Arc::new(LoraWifiBeacon::new(name, id, sr_sender, lr_sender, rng, logger));

            //Build the resources context
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }
        Protocols::Flooding => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio
                .expect("The NaiveRouting protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(Flooding::new(
                name,
                id,
                sr_sender.clone(),
                Arc::new(Mutex::new(rng)),
                logger,
            ));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, (Arc::clone(&sr), sr_sender, sr_receiver)));
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }
        Protocols::Gossip { k, p } => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio
                .expect("The GossipRouting protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(GossipRouting::new(
                name,
                id,
                k,
                p,
                sr_sender.clone(),
                Arc::new(Mutex::new(rng)),
                logger,
            ));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, (Arc::clone(&sr), sr_sender, sr_receiver)));
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }
        Protocols::RGR { k, p } => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) = short_radio
                .expect("The ReactiveGossip protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(ReactiveGossipRouting::new(
                name,
                id,
                k,
                p,
                sr_sender.clone(),
                Arc::new(Mutex::new(rng)),
                logger,
            ));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, (Arc::clone(&sr), sr_sender, sr_receiver)));
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }
        Protocols::RGRII { k, p, q } => {
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
                sr_sender.clone(),
                Arc::new(Mutex::new(rng)),
                logger,
            ));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, (Arc::clone(&sr), sr_sender, sr_receiver)));
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }
        Protocols::RGRIII { k, p, q, beacon_threshold } => {
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
                beacon_threshold,
                sr_sender.clone(),
                lr_sender.clone(),
                Arc::new(Mutex::new(rng)),
                logger,
            ));
            let mut radio_channels = Vec::new();
            radio_channels.push((sr_listener, (Arc::clone(&sr), sr_sender, sr_receiver)));
            radio_channels.push((lr_listener, (Arc::clone(&lr), lr_sender, lr_receiver)));

            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }
        Protocols::AODV {
            active_route_timeout,
            net_diameter,
            node_traversal_time,
            allowed_hello_loss,
            rreq_retries,
            next_hop_wait,
        } => {
            //Obtain the short-range radio. For this protocol, the long-range radio is ignored.
            let (sr, listener) =
                short_radio.expect("The AODV protocol requires a short_radio to be provided.");
            let rng = Worker::rng_from_seed(seed);
            let handler: Arc<dyn Protocol> = Arc::new(AODV::new(
                name,
                id,
                active_route_timeout,
                net_diameter,
                node_traversal_time,
                next_hop_wait,
                allowed_hello_loss,
                rreq_retries,
                sr_sender.clone(),
                Arc::new(Mutex::new(rng)),
                sr.last_transmission(),
                logger,
            ));
            let mut radio_channels = Vec::new();
            radio_channels.push((listener, (Arc::clone(&sr), sr_sender, sr_receiver)));
            let resources = ProtocolResources {
                handler,
                radio_channels,
            };
            Ok(resources)
        }
    }
}
