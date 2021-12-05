//! This module implements the Reactive Gossip routing protocol

use crate::worker::protocols::{Transmission, HandleMessageOutcome, Protocol, ProtocolMessages};
use crate::worker::radio::{self, *};
use crate::worker::{MessageHeader, MessageStatus};
use crate::{MeshSimError, MeshSimErrorKind};

use rand::{rngs::StdRng, Rng};
// use serde_cbor::de::*;
// use serde_cbor::ser::*;
use slog::{Logger, Record, Serializer, KV};

use chrono::{DateTime, Duration, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use crossbeam_channel::Sender;

/// The default number of hops messages are guaranteed to be propagated
pub const DEFAULT_MIN_HOPS: usize = 2;
/// The default gossip-probability value
pub const DEFAULT_GOSSIP_PROB: f64 = 0.70;
const MSG_CACHE_SIZE: usize = 2000;
const CONCCURENT_THREADS_PER_FLOW: usize = 2;
const MAINTENANCE_DATA_RETRANSMISSION: i64 = 1_000;
const MAINTENANCE_RD_RETRANSMISSION: i64 = 1_000;
const ROUTE_DISCOVERY_THRESHOLD: i64 = 5000;
const ROUTE_DISCOVERY_MAX_RETRY: usize = 3;
const MAX_PACKET_RETRANSMISSION: usize = 3;

/// Used for the pending_destination table
type PendingRouteEntry = (String, DateTime<Utc>, usize);

/// Main structure used in this protocol
#[derive(Debug)]
pub struct ReactiveGossipRouting {
    /// Configuration parameter for the protocol that indicates the minimum number of hops a message
    /// has to traverse before applying the gossip probability.
    k: usize,
    /// Gossip probability per node
    p: f64,
    worker_name: String,
    worker_id: String,
    wifi_tx_queue: Sender<Transmission>,
    /// Used for rapid decision-making of whether to forward a data message or not.
    known_routes: Arc<Mutex<HashMap<String, bool>>>,
    /// Destinations for which a route-discovery process has started but not yet finished.
    pending_destinations: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
    /// Used to cache recent route-discovery messages received.
    route_msg_cache: Arc<Mutex<HashSet<String>>>,
    /// Used to cache recent data messaged received.
    data_msg_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
    /// The map is indexed by destination (worker_name) and the value is the associated
    /// route_id for that route.
    destination_routes: Arc<Mutex<HashMap<String, String>>>,
    /// Map to keep track of pending transmissions that are waiting for their associated route_id to be
    /// established.
    queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
    /// Timestamp for the next scheduled data retransmission operation
    ts_data_retransmission: AtomicI64,
    /// Timestamp for the next scheduled route-discovery retransmission operation
    ts_rd_retransmission: AtomicI64,
    /// RNG used for routing calculations
    rng: Arc<Mutex<StdRng>>,
    /// The logger to use in this protocol
    logger: Logger,
}

/// This struct contains the state necessary for the protocol to discover routes between
/// two nodes and if one or more exist, establish said routes.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RouteMessage {
    pub route_source: String,
    pub route_destination: String,
    pub route_id: String,
    pub route: Vec<String>,
}

impl RouteMessage {
    fn new(source: String, destination: String) -> RouteMessage {
        let mut data = Vec::new();
        data.append(&mut source.clone().into_bytes());
        data.append(&mut destination.clone().into_bytes());
        //Add timestamp to differentiate between retries
        let mut ts = Utc::now().timestamp_nanos().to_le_bytes().to_vec();
        data.append(&mut ts);

        let dig = md5::compute(&data);
        let route_id = format!("{:x}", dig);

        RouteMessage {
            route_source: source,
            route_destination: destination,
            route_id,
            route: vec![],
        }
    }
}

///This message uses an already established route to send data to the destination
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataMessage {
    pub payload: Vec<u8>,
    pub route_id: String,
}

/// Used in the data message cache to manage retransmissions
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum DataMessageStates {
    /// Have not yet confirmed this message has been relayed by a neighboring node
    Pending(String),
    /// Message has been relayed by a neighboring node or reached its destination.
    Confirmed,
}

impl DataMessageStates {
    fn is_pending(&self) -> bool {
        match self {
            DataMessageStates::Pending(_) => true,
            _ => false,
        }
    }
}

/// Used in the data message cache to manage retransmissions
#[derive(Debug, Serialize, Deserialize)]
pub struct DataCacheEntry {
    state: DataMessageStates,
    retries: usize,
    payload: MessageHeader,
}

/// Enum that lists all the possible messages in this protocol as well as the associated data for each one
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Messages {
    RouteDiscovery(RouteMessage),
    RouteEstablish(RouteMessage),
    RouteTeardown(RouteMessage),
    Data(DataMessage),
}

impl KV for Messages {
    fn serialize(&self, _rec: &Record, serializer: &mut dyn Serializer) -> slog::Result {
        match *self {
            Messages::Data(ref m) => {
                let _ = serializer.emit_str("msg_type", "DATA")?;
                serializer.emit_str("route_id", &m.route_id)
            }
            Messages::RouteDiscovery(ref m) => {
                let _ = serializer.emit_str("msg_type", "ROUTE_DISCOVERY")?;
                let _ = serializer.emit_str("route_id", &m.route_id)?;
                let _ = serializer.emit_str("msg_source", &m.route_source)?;
                let _ = serializer.emit_str("msg_destination", &m.route_destination)?;
                serializer.emit_usize("route_length", m.route.len())
            }
            Messages::RouteEstablish(ref m) => {
                let _ = serializer.emit_str("msg_type", "ROUTE_ESTABLISH")?;
                let _ = serializer.emit_str("route_id", &m.route_id)?;
                let _ = serializer.emit_str("msg_source", &m.route_source)?;
                let _ = serializer.emit_str("msg_destination", &m.route_destination)?;
                serializer.emit_usize("route_length", m.route.len())
            }
            Messages::RouteTeardown(ref m) => {
                let _ = serializer.emit_str("msg_type", "ROUTE_TEARDOWN")?;
                serializer.emit_str("route_id", &m.route_id)
            }
        }
    }
}

impl Protocol for ReactiveGossipRouting {
    fn handle_message(
        &self,
        hdr: MessageHeader,
        _ts: DateTime<Utc>,
        r_type: RadioTypes,
    ) -> Result<(), MeshSimError> {
        let msg_id = hdr.get_msg_id().to_string();
        let msg = deserialize_message(hdr.get_payload())?;
        let queued_transmissions = Arc::clone(&self.queued_transmissions);
        let dest_routes = Arc::clone(&self.destination_routes);
        let pending_destinations = Arc::clone(&self.pending_destinations);
        let known_routes = Arc::clone(&self.known_routes);
        let self_peer = self.get_self_peer();
        let wifi_tx_queue = self.wifi_tx_queue.clone();
        let route_msg_cache = Arc::clone(&self.route_msg_cache);
        let data_msg_cache = Arc::clone(&self.data_msg_cache);
        let rng = Arc::clone(&self.rng);
        let resp = ReactiveGossipRouting::handle_message_internal(
            hdr,
            msg,
            self_peer,
            msg_id,
            self.k,
            self.p,
            queued_transmissions,
            dest_routes,
            pending_destinations,
            known_routes,
            route_msg_cache,
            data_msg_cache,
            wifi_tx_queue,
            r_type,
            &self.logger,
            rng,
        )?;

        if let Some((resp_hdr, md)) = resp {
            self.wifi_tx_queue.send((resp_hdr, md, Utc::now()))
            .map_err(|e| { 
                let msg = "Failed to queue response into tx_queue".to_string();
                MeshSimError {
                    kind: MeshSimErrorKind::Contention(msg),
                    cause: Some(Box::new(e)),
                }
            })?;
        }

        Ok(())
    }

    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError> {

        Ok(None)
    }

    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError> {
        let perf_out_queued_start = Utc::now();
        let me = self.get_self_peer();
        let route_id = {
            let routes = self
                .destination_routes
                .lock()
                .expect("Failed to lock destination_routes table");
            routes.get(&destination).map(|v| v.to_owned())
        };

        //Check if an available route exists for the destination.
        if let Some(r_id) = route_id {
            let msg = Messages::Data(DataMessage {
                route_id: r_id.clone(),
                payload: data,
            });
            let log_data = ProtocolMessages::RGRI(msg.clone());
            let mut hdr = MessageHeader::new(me, destination, serialize_message(msg)?);
            hdr.delay = perf_out_queued_start.timestamp_nanos();

            info!(&self.logger, "New message"; "msg_id" => hdr.get_msg_id());

            ReactiveGossipRouting::start_flow(
                r_id,
                log_data,
                hdr,
                self.wifi_tx_queue.clone(),
                Arc::clone(&self.data_msg_cache),
                &self.logger,
            )?;
        } else {
            let route_id = {
                let mut pending_destinations = self
                    .pending_destinations
                    .lock()
                    .expect("Failed to lock pending_destinations table");

                match pending_destinations.get(&destination) {
                    Some((route_id, _ts, _tries)) => {
                        info!(
                            self.logger,
                            "Route discovery process already started for {}", &destination
                        );
                        route_id.to_owned()
                    }
                    None => {
                        info!(
                            self.logger,
                            "No known route to {}. Starting discovery process.", &destination
                        );
                        let route_id = ReactiveGossipRouting::start_route_discovery(
                            &me,
                            destination.clone(),
                            self.wifi_tx_queue.clone(),
                            Arc::clone(&self.route_msg_cache),
                            Arc::clone(&self.queued_transmissions),
                            &self.logger,
                        )?;
                        let expiration =
                            Utc::now() + chrono::Duration::milliseconds(ROUTE_DISCOVERY_THRESHOLD);
                        let entry = pending_destinations
                            .entry(destination.clone())
                            .or_insert_with(|| (route_id, expiration, 0));

                        entry.0.to_owned()
                    }
                }
            };

            //Now that we have a route_id, build a data message.
            let msg = Messages::Data(DataMessage {
                route_id,
                payload: data,
            });
            let mut hdr = MessageHeader::new(me, destination.clone(), serialize_message(msg)?);
            hdr.delay = perf_out_queued_start.timestamp_nanos();
            info!(&self.logger, "New message"; "msg_id" => hdr.get_msg_id());

            //...and then queue the data transmission for when the route is established
            let qt = Arc::clone(&self.queued_transmissions);
            ReactiveGossipRouting::queue_transmission(qt, destination, hdr)?;
        }
        Ok(())
    }

    fn do_maintenance(&self) -> Result<(), MeshSimError> {
        if Utc::now().timestamp_nanos() >= self.ts_data_retransmission.load(Ordering::SeqCst) {
            match ReactiveGossipRouting::data_retransmission(
                Arc::clone(&self.data_msg_cache),
                Arc::clone(&self.destination_routes),
                Arc::clone(&self.known_routes),
                self.get_self_peer(),
                self.wifi_tx_queue.clone(),
                &self.logger,
            ) {
                Ok(_) => {
                    //Update to the new timestamp for doing this maintenance operation
                    let new_ts =
                        Utc::now() + Duration::milliseconds(MAINTENANCE_DATA_RETRANSMISSION);
                    self.ts_data_retransmission
                        .store(new_ts.timestamp_nanos(), Ordering::SeqCst);
                }
                Err(e) => {
                    error!(
                        &self.logger,
                        "Failed to retransmit Data message";
                        "reason"=>format!("{}",e)
                    );
                }
            }
        }

        if Utc::now().timestamp_nanos() >= self.ts_rd_retransmission.load(Ordering::SeqCst) {
            match ReactiveGossipRouting::process_expired_route_discoveries(
                Arc::clone(&self.pending_destinations),
                Arc::clone(&self.queued_transmissions),
                Arc::clone(&self.route_msg_cache),
                self.get_self_peer(),
                self.wifi_tx_queue.clone(),
                &self.logger,
            ) {
                Ok(_) => {
                    //Update to the new timestamp for doing this maintenance operation
                    let new_ts = Utc::now() + Duration::milliseconds(MAINTENANCE_RD_RETRANSMISSION);
                    self.ts_rd_retransmission
                        .store(new_ts.timestamp_nanos(), Ordering::SeqCst);
                }
                Err(e) => {
                    error!(
                        &self.logger,
                        "Failed to re-transmit route discovery";
                        "reason"=>format!("{}",e)
                    );
                }
            }
        }

        Ok(())
    }
}

impl ReactiveGossipRouting {
    /// Creates a new instance of the protocol.
    pub fn new(
        worker_name: String,
        worker_id: String,
        k: Option<usize>,
        p: Option<f64>,
        wifi_tx_queue: Sender<Transmission>,
        rng: Arc<Mutex<StdRng>>,
        logger: Logger,
    ) -> ReactiveGossipRouting {
        let qt = HashMap::new();
        let d_routes = HashMap::new();
        let k_routes = HashMap::new();
        let route_cache = HashSet::new();
        let data_cache = HashMap::new();
        let pending_destinations = HashMap::new();
        let ts_data_retransmission =
            Utc::now() + Duration::milliseconds(MAINTENANCE_DATA_RETRANSMISSION);
        let ts_data_retransmission = AtomicI64::new(ts_data_retransmission.timestamp_nanos());
        let ts_rd_retransmission =
            Utc::now() + Duration::milliseconds(MAINTENANCE_RD_RETRANSMISSION);
        let ts_rd_retransmission = AtomicI64::new(ts_rd_retransmission.timestamp_nanos());

        let k = k.unwrap_or(DEFAULT_MIN_HOPS);
        let p = p.unwrap_or(DEFAULT_GOSSIP_PROB);
        
        ReactiveGossipRouting {
            k,
            p,
            worker_name,
            worker_id,
            wifi_tx_queue,
            destination_routes: Arc::new(Mutex::new(d_routes)),
            queued_transmissions: Arc::new(Mutex::new(qt)),
            known_routes: Arc::new(Mutex::new(k_routes)),
            pending_destinations: Arc::new(Mutex::new(pending_destinations)),
            route_msg_cache: Arc::new(Mutex::new(route_cache)),
            data_msg_cache: Arc::new(Mutex::new(data_cache)),
            ts_data_retransmission,
            ts_rd_retransmission,
            rng,
            logger,
        }
    }

    fn start_route_discovery(
        me: &String,
        destination: String,
        wifi_tx_queue: Sender<Transmission>,
        route_msg_cache: Arc<Mutex<HashSet<String>>>,
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        logger: &Logger,
    ) -> Result<String, MeshSimError> {
        let mut msg = RouteMessage::new(me.clone(), destination.clone());
        msg.route.push(me.clone());
        let route_id = msg.route_id.clone();
        let msg = Messages::RouteDiscovery(msg);
        let log_data = ProtocolMessages::RGRI(msg.clone());
        let hdr = MessageHeader::new(me.clone(), destination.clone(), serialize_message(msg)?);

        //TODO: Investigate if this is the reason RGR variations are underperforming
        //For the case when the route_discovery is triggered because the previous one failed, we may have cached messages
        //for which the payload holds the old RouteID. If so, this funciton will update those messages. Otherwise, it's a noop.
        ReactiveGossipRouting::update_queued_transmissions(
            queued_transmissions,
            &destination,
            route_id.clone(),
        )?;

        //Add the route to the route cache so that this node does not relay it again
        {
            let mut rmc = route_msg_cache
                .lock()
                .expect("Could not lock route_message cache");
            rmc.insert(route_id.clone());
        }

        wifi_tx_queue.send((hdr, log_data, Utc::now()))
        .map_err(|e| { 
            let msg = "Failed to queue response into tx_queue".to_string();
            MeshSimError {
                kind: MeshSimErrorKind::Contention(msg),
                cause: Some(Box::new(e)),
            }
        })?;

        //TODO: If there are issues with route discovery, check here, since it may be reported to have started, but the packet is queued.
        info!(
            logger,
            "Route discovery initiated";
            "route_id"=>&route_id,
            "destination"=>&destination,
        );
        
        Ok(route_id)
    }

    fn start_flow(
        route_id: String,
        log_data: ProtocolMessages,
        hdr: MessageHeader,
        wifi_tx_queue: Sender<Transmission>,
        data_msg_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        _logger: &Logger,
    ) -> Result<(), MeshSimError> {
        //Log this message in the data_msg_cache so that we can monitor if the neighbors relay it
        //properly, retransmit if necessary, and don't relay it again when we hear it from others.
        let mut dc = data_msg_cache
            .lock()
            .expect("Failed to lock data_message cache");
        let msg_id = hdr.get_msg_id().to_string();
        dc.insert(
            msg_id,
            DataCacheEntry {
                state: DataMessageStates::Pending(route_id),
                retries: 0,
                payload: hdr.clone(),
            },
        );
        //Right now this assumes the data can be sent in a single broadcast message
        //This might be addressed later on.
        wifi_tx_queue.send((hdr, log_data, Utc::now()))
        .map_err(|e| { 
            let msg = "Failed to queue response into tx_queue".to_string();
            MeshSimError {
                kind: MeshSimErrorKind::Contention(msg),
                cause: Some(Box::new(e)),
            }
        })?;

        Ok(())
    }

    fn queue_transmission(
        trans_q: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        destination: String,
        data: MessageHeader,
    ) -> Result<(), MeshSimError> {
        let mut tq = trans_q
            .lock()
            .expect("Error trying to acquire lock to transmissions queue");
        let e = tq.entry(destination).or_insert_with(|| vec![]);
        e.push(data);
        Ok(())
    }

    fn handle_message_internal(
        hdr: MessageHeader,
        msg: Messages,
        self_peer: String,
        msg_id: String,
        k: usize,
        p: f64,
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        dest_routes: Arc<Mutex<HashMap<String, String>>>,
        pending_destinations: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
        known_routes: Arc<Mutex<HashMap<String, bool>>>,
        route_msg_cache: Arc<Mutex<HashSet<String>>>,
        data_msg_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        wifi_tx_queue: Sender<Transmission>,
        r_type: RadioTypes,
        logger: &Logger,
        rng: Arc<Mutex<StdRng>>,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        match msg {
            Messages::Data(data_msg) => {
                // debug!(logger, "Received DATA message");
                ReactiveGossipRouting::process_data_msg(
                    hdr,
                    data_msg,
                    known_routes,
                    data_msg_cache,
                    self_peer,
                    msg_id,
                    r_type,
                    logger,
                )
            }
            Messages::RouteDiscovery(route_msg) => {
                //                        debug!(logger, "Received ROUTE_DISCOVERY message");
                ReactiveGossipRouting::process_route_discovery_msg(
                    hdr,
                    route_msg,
                    k,
                    p,
                    known_routes,
                    route_msg_cache,
                    self_peer,
                    msg_id,
                    rng,
                    r_type,
                    logger,
                )
            }
            Messages::RouteEstablish(route_msg) => {
                //                        debug!(logger, "Received ROUTE_ESTABLISH message");
                ReactiveGossipRouting::process_route_established_msg(
                    hdr,
                    route_msg,
                    known_routes,
                    dest_routes,
                    pending_destinations,
                    queued_transmissions,
                    data_msg_cache,
                    self_peer,
                    msg_id,
                    wifi_tx_queue,
                    r_type,
                    logger,
                )
            }
            Messages::RouteTeardown(route_msg) => {
                ReactiveGossipRouting::process_route_teardown_msg(
                    hdr,
                    route_msg,
                    known_routes,
                    dest_routes,
                    self_peer,
                    r_type,
                    logger,
                )
            }
        }
    }

    fn process_data_msg(
        mut hdr: MessageHeader,
        msg: DataMessage,
        known_routes: Arc<Mutex<HashMap<String, bool>>>,
        data_msg_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        self_peer: String,
        msg_id: String,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        let route_id = msg.route_id.clone();
        let part_of_route = {
            let known_routes = known_routes
                .lock()
                .expect("Failed to lock known_routes table");
            known_routes.get(&msg.route_id).copied()
        };

        //Are we part of this route?
        let last_hop = match part_of_route {
            Some(last_hop) => last_hop,
            None => {
                // info!(
                //     logger,
                //     "Received message";
                //     "msg_id"=>&msg_id,
                //     "msg_type"=>"DATA",
                //     "sender"=>&hdr.sender,
                //     "status"=>MessageStatus::DROPPED,
                //     "reason"=>"Not part of route",
                // );
                radio::log_handle_message(
                    logger,
                    &hdr,
                    MessageStatus::DROPPED,
                    Some("Not part of route"),
                    None,
                    r_type,
                    &Messages::Data(msg),
                );
                return Ok(None);
            }
        };

        //We are part of the route then.
        {
            let mut d_cache = data_msg_cache
                .lock()
                .expect("Failed to lock data_message cache");

            //Is this a new message?
            if let Some((cached_id, mut entry)) = d_cache.remove_entry(&msg_id) {
                match entry.state {
                    DataMessageStates::Pending(_route_id) => {
                        entry.state = DataMessageStates::Confirmed;
                        //TODO: Should the packet copy be dropped here?
                        radio::log_handle_message(
                            logger,
                            &hdr,
                            MessageStatus::DROPPED,
                            Some("CONFIRMED"),
                            None,
                            r_type,
                            &Messages::Data(msg),
                        );
                    }
                    DataMessageStates::Confirmed => {
                        radio::log_handle_message(
                            logger,
                            &hdr,
                            MessageStatus::DROPPED,
                            Some("DUPLICATE"),
                            None,
                            r_type,
                            &Messages::Data(msg),
                        );
                    }
                }
                d_cache.insert(cached_id, entry);
                return Ok(None);
            } else {
                //This is a new message
                //Is there space in the cache?
                if d_cache.len() >= MSG_CACHE_SIZE {
                    let e: String = d_cache
                        .keys()
                        .take(1) //Select the first random element to remove.
                        .next() //Take it.
                        .unwrap()
                        .to_owned(); //Copy the data so we can get a mutable borrow later.
                    d_cache.remove(&e);
                }
                let state = {
                    if last_hop {
                        DataMessageStates::Confirmed
                    } else {
                        DataMessageStates::Pending(route_id)
                    }
                };
                // re-package the message for caching in case it needs to be retransmitted
                hdr.payload = serialize_message(Messages::Data(msg.clone()))?;
                d_cache.insert(
                    msg_id.clone(),
                    DataCacheEntry {
                        state,
                        retries: 0,
                        payload: hdr.clone(),
                    },
                );
            }
        }

        //Are the intended recipient?
        if hdr.destination == self_peer {
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::ACCEPTED,
                None,
                None,
                r_type,
                &Messages::Data(msg),
            );
            Ok(None)
        } else {
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::FORWARDING,
                None,
                None,
                r_type,
                &Messages::Data(msg.clone()),
            );

            //...and re-package the message.
            // hdr.payload = serialize_message(Messages::Data(msg))?;
            //Since the payload remains unchanged, just create new header with this node as the sender.
            let fwd_hdr = hdr.create_forward_header(self_peer).build();
            let log_data = ProtocolMessages::RGRI(Messages::Data(msg));

            Ok(Some((fwd_hdr, log_data)))
        }
    }

    fn process_route_discovery_msg(
        hdr: MessageHeader,
        mut msg: RouteMessage,
        k: usize,
        p: f64,
        known_routes: Arc<Mutex<HashMap<String, bool>>>,
        route_msg_cache: Arc<Mutex<HashSet<String>>>,
        self_peer: String,
        _msg_id: String,
        rng: Arc<Mutex<StdRng>>,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        //Is this node the intended destination of the route?
        if msg.route_destination == self_peer {
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::ACCEPTED,
                Some("Route discovery succeeded"),
                Some("Start RouteEstablish process"),
                r_type,
                &Messages::RouteDiscovery(msg.clone()),
            );
            //Add this route to the list of known routes.
            //This is now needed in the current version of the protocol as the process_data_msg
            //function discriminates on route-membership first of all. Also, must add this route as
            //"last-hop" even though technically it isn't, since we dont need to confirm the data
            //messages that come through it, since it won't be relaying them (this is the destination).
            {
                let mut kr = known_routes
                    .lock()
                    .expect("Failed to lock known_routes table");
                kr.insert(msg.route_id.clone(), true);
            }

            //Update route
            msg.route.insert(0, self_peer.clone());
            //Extract route destination
            let dest = msg.route_source.clone();
            //Re-tag the message
            let msg = Messages::RouteEstablish(msg);
            //Create logging data
            let log_data = ProtocolMessages::RGRI(msg.clone());

            //Create response header
            let response_hdr = MessageHeader::new(self_peer, dest, serialize_message(msg)?);

            return Ok(Some((response_hdr, log_data)));
        }

        //Is this a new route?
        {
            let mut p_routes = route_msg_cache
                .lock()
                .expect("Failed to lock route_message cache");
            if !p_routes.insert(msg.route_id.clone()) {
                radio::log_handle_message(
                    logger,
                    &hdr,
                    MessageStatus::DROPPED,
                    Some("DUPLICATE"),
                    None,
                    r_type,
                    &Messages::RouteDiscovery(msg),
                );
                return Ok(None);
            }
        }

        //Gossip?
        let s: f64 = {
            let mut rng = rng.lock().expect("Could not obtain lock for RNG");
            rng.gen_range(0f64..1f64)
        };
        debug!(logger, "Gossip prob {}", s);
        if msg.route.len() > k && s > p {
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::DROPPED,
                Some("Gossip failed"),
                None,
                r_type,
                &Messages::RouteDiscovery(msg),
            );
            //Not gossiping this message.
            return Ok(None);
        }

        //Update route
        msg.route.push(self_peer.clone());
        //Re-tag the message
        let msg = Messages::RouteDiscovery(msg);

        radio::log_handle_message(logger, &hdr, MessageStatus::FORWARDING, None, None, r_type, &msg);
        // Build log data
        let log_data = ProtocolMessages::RGRI(msg.clone());
        //Build message and forward it
        let fwd_hdr = hdr
            .create_forward_header(self_peer)
            .set_payload(serialize_message(msg)?)
            .build();

        Ok(Some((fwd_hdr, log_data)))
    }

    fn process_route_established_msg(
        hdr: MessageHeader,
        mut msg: RouteMessage,
        known_routes: Arc<Mutex<HashMap<String, bool>>>,
        dest_routes: Arc<Mutex<HashMap<String, String>>>,
        pending_destinations: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        data_msg_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        self_peer: String,
        _msg_id: String,
        wifi_tx_queue: Sender<Transmission>,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        //Who's the next hop in the route?
        let next_hop = match msg.route.pop() {
            Some(h) => h,
            None => {
                radio::log_handle_message(
                    logger,
                    &hdr,
                    MessageStatus::DROPPED,
                    Some("Empty payload"),
                    None,
                    r_type,
                    &Messages::RouteEstablish(msg),
                );
                return Ok(None);
            }
        };
        //Is the message meant for this node?
        if next_hop != self_peer {
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::DROPPED,
                Some("Not meant for this node"),
                None,
                r_type,
                &Messages::RouteEstablish(msg),
            );
            return Ok(None);
        }

        //Add the this route to the known_routes list...
        {
            let mut kr = known_routes
                .lock()
                .expect("Failed to lock known_routes table");

            //Indicate whether this node is the last hop in the route to the destination.
            //This will be used by the node to know whether it has to verify packets it forwards,
            //since th destination will not relay them.
            let last_hop = msg.route_destination == msg.route[0];
            let _res = kr.insert(msg.route_id.clone(), last_hop);
        }
        //...and the destination-route list
        {
            let mut dr = dest_routes
                .lock()
                .expect("Failed to lock destination_routes table");
            let _res = dr.insert(msg.route_destination.clone(), msg.route_id.clone());
        }

        //This is the next hop in the route. Update route to reflect this.
        msg.route.insert(0, next_hop);

        debug!(logger, "Known_routes: {:?}", &known_routes);
        debug!(logger, "Destination_routes: {:?}", &dest_routes);

        //Is this the source of the route?
        debug!(logger, "Route_Source:{}", &msg.route_source);
        if msg.route_source == self_peer {
            let route = format!("Route: {:?}", &msg.route);
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::ACCEPTED,
                None,
                None,
                r_type,
                &Messages::RouteEstablish(msg.clone()),
            );
            info!(logger, "route {}:{}", &msg.route_id, route);

            //...and remove this node from the pending destinations
            {
                let mut pd = pending_destinations
                    .lock()
                    .expect("Error trying to acquire lock to pending_destinations table");
                let _res = pd.remove(&msg.route_destination);
                debug!(logger, "Pending Destinations Table: {:?}", &pd);
            }

            //Start any flows that were waiting on the route
            let _ = ReactiveGossipRouting::start_queued_flows(
                queued_transmissions,
                msg.route_id.clone(),
                msg.route_destination,
                self_peer,
                wifi_tx_queue,
                data_msg_cache,
                logger,
            );
            return Ok(None);
        }

        //Re-tag the message
        let msg = Messages::RouteEstablish(msg);

        radio::log_handle_message(logger, &hdr, MessageStatus::FORWARDING, None, None, r_type, &msg);

        //Build log data
        let log_data = ProtocolMessages::RGRI(msg.clone());

        //Build message and forward it
        let fwd_hdr = hdr
            .create_forward_header(self_peer)
            .set_payload(serialize_message(msg)?)
            .build();

        Ok(Some((fwd_hdr, log_data)))
    }

    fn process_route_teardown_msg(
        hdr: MessageHeader,
        msg: RouteMessage,
        known_routes: Arc<Mutex<HashMap<String, bool>>>,
        dest_routes: Arc<Mutex<HashMap<String, String>>>,
        self_peer: String,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        let subscribed = {
            let kr = known_routes
                .lock()
                .expect("Error trying to acquire lock to known_routes table");
            kr.contains_key(&msg.route_id)
        };

        let resp: HandleMessageOutcome = {
            if subscribed {
                //Log incoming packet
                radio::log_handle_message(
                    logger,
                    &hdr,
                    MessageStatus::FORWARDING,
                    None,
                    None,
                    r_type,
                    &Messages::RouteTeardown(msg.clone()),
                );
                //Teardown relevant routes and build new msg
                let msg = ReactiveGossipRouting::route_teardown(
                    &msg.route_id,
                    &self_peer,
                    dest_routes,
                    known_routes,
                    logger,
                )?;
                //Create log data
                let msg = Messages::RouteTeardown(msg);
                let log_data = ProtocolMessages::RGRI(msg.clone());
                let fwd_hdr = hdr
                    .create_forward_header(self_peer)
                    .set_payload(serialize_message(msg)?)
                    .build();
                Some((fwd_hdr, log_data))
            } else {
                radio::log_handle_message(
                    logger,
                    &hdr,
                    MessageStatus::DROPPED,
                    Some("Not subscribed to route"),
                    None,
                    r_type,
                    &Messages::RouteTeardown(msg),
                );
                None
            }
        };
        Ok(resp)
    }

    fn start_queued_flows(
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        route_id: String,
        destination: String,
        _self_peer: String,
        wifi_tx_queue: Sender<Transmission>,
        data_msg_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        info!(logger, "Looking for queued flows for {}", &destination);

        let entry: Option<(String, Vec<MessageHeader>)> = {
            let mut qt = queued_transmissions
                .lock()
                .expect("Error trying to acquire lock to transmissions queue");
            qt.remove_entry(&destination)
        };

        let thread_pool = threadpool::Builder::new()
            .num_threads(CONCCURENT_THREADS_PER_FLOW)
            .build();
        if let Some((_key, flows)) = entry {
            info!(logger, "Processing {} queued transmissions.", &flows.len());
            for hdr in flows {
                let r_id = route_id.clone();
                let log_data = ProtocolMessages::RGRI(deserialize_message(hdr.get_payload())?);
                let tx_queue = wifi_tx_queue.clone();
                let l = logger.clone();
                let dmc = Arc::clone(&data_msg_cache);
                thread_pool.execute(move || {
                    match ReactiveGossipRouting::start_flow(
                        r_id.clone(),
                        log_data,
                        hdr,
                        tx_queue,
                        dmc,
                        &l,
                    ) {
                        Ok(_) => {
                            // All good!
                        }
                        Err(e) => {
                            error!(l, "Failed to start transmission for route {} - {}", r_id, e);
                        }
                    }
                });
            }
        }

        //Wait for all threads to finish
        thread_pool.join();
        Ok(())
    }

    fn get_self_peer(&self) -> String {
        self.worker_name.clone()
    }

    fn data_retransmission(
        data_msg_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        destination_routes: Arc<Mutex<HashMap<String, String>>>,
        known_routes: Arc<Mutex<HashMap<String, bool>>>,
        me: String,
        wifi_tx_queue: Sender<Transmission>,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        let mut cache = data_msg_cache
            .lock()
            .expect("Error trying to acquire lock to data_messages cache");
        for (msg_hash, entry) in cache
            .iter_mut()
            .filter(|(_msg_hash, entry)| entry.state.is_pending())
        {
            debug!(logger, "Message {} is pending confirmation", &msg_hash);

            if entry.retries < MAX_PACKET_RETRANSMISSION {
                let hdr = entry.payload.clone();
                entry.retries += 1;
                let log_data = ProtocolMessages::RGRI(deserialize_message(hdr.get_payload())?);
                info!(
                    logger,
                    "Retransmitting message {}", &hdr.get_msg_id();
                    "retries"=>entry.retries,
                );

                //The message is still cached, so re-transmit it.
                wifi_tx_queue.send((hdr, log_data, Utc::now()))
                .map_err(|e| { 
                    let msg = "Failed to queue response into tx_queue".to_string();
                    MeshSimError {
                        kind: MeshSimErrorKind::Contention(msg),
                        cause: Some(Box::new(e)),
                    }
                })?;

            } else {
                // The message is no longer cached.
                // At this point, we assume the route is broken.
                // Send the route teardown message.
                if let DataMessageStates::Pending(ref route_id) = entry.state.clone() {
                    // info!(logger, "Starting route {} teardown", route_id);
                    //Mark this entry in the message cache as confirmed so that we don't
                    //process it again.
                    entry.state = DataMessageStates::Confirmed;

                    //Tear down the route in the internal caches
                    let msg = ReactiveGossipRouting::route_teardown(
                        route_id,
                        &me,
                        Arc::clone(&destination_routes),
                        Arc::clone(&known_routes),
                        &logger,
                    )?;
                    let msg = Messages::RouteTeardown(msg);
                    //Build log data from the packet
                    let log_data = ProtocolMessages::RGRI(msg.clone());
                    //Pack the message and build the header for transmission
                    let hdr =
                        MessageHeader::new(me.clone(), String::new(), serialize_message(msg)?);
                    //Send message
                    wifi_tx_queue.send((hdr, log_data, Utc::now()))
                    .map_err(|e| { 
                        let msg = "Failed to queue response into tx_queue".to_string();
                        MeshSimError {
                            kind: MeshSimErrorKind::Contention(msg),
                            cause: Some(Box::new(e)),
                        }
                    })?;
                    info!(
                        logger,
                        "Route Teardown initiated";
                        "route_id"=>&route_id,
                        "reason"=>"Unable to confirm previously sent DATA message"
                    );

                } else {
                    error!(logger, "Inconsistent state reached");
                    unreachable!();
                }
            }
        }
        Ok(())
    }

    fn process_expired_route_discoveries(
        pending_destinations: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        route_msg_cache: Arc<Mutex<HashSet<String>>>,
        me: String,
        wifi_tx_queue: Sender<Transmission>,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        let mut expired_discoveries = Vec::new();
        //Route expiration retransmission
        {
            let mut pd = pending_destinations
                .lock()
                .expect("Failed to lock pending_destinations table");
            for (dest, (route_id, ts, retries)) in
                pd.iter_mut().filter(|(_dest, entry)| entry.1 < Utc::now())
            {
                //Should we retry?
                if *retries < ROUTE_DISCOVERY_MAX_RETRY {
                    info!(
                        logger,
                        "Re-trying ROUTE_DISCOVERY for {}", &dest;
                        "reason"=>"Time limit for discover exceeded"
                    );
                    //Transmit new route discovery
                    let new_route_id = ReactiveGossipRouting::start_route_discovery(
                        &me,
                        dest.clone(),
                        wifi_tx_queue.clone(),
                        Arc::clone(&route_msg_cache),
                        Arc::clone(&queued_transmissions),
                        &logger,
                    )?;

                    //Increase retries for route-discovery
                    *retries += 1;
                    *route_id = new_route_id;
                    *ts = Utc::now() + chrono::Duration::milliseconds(ROUTE_DISCOVERY_THRESHOLD);
                } else {
                    //Retries exhausted. Add to list.
                    expired_discoveries.push(dest.clone());
                    warn!(
                        logger,
                        "Dropping queued packets for {}", &dest;
                        "reason"=>"ROUTE_DISCOVERY retries exceeded",
                        "route"=>&(*route_id),
                    );
                }
            }
            //Remove the expired entries from the pending_destinations list.
            pd.retain(|dest, _| !expired_discoveries.contains(dest));
        }

        //Purge queued packets of expired routes
        {
            let mut qt = queued_transmissions
                .lock()
                .expect("Failed to lock queued_transmissions queue");
            qt.retain(|dest, _| !expired_discoveries.contains(dest));
        }

        Ok(())
    }

    fn route_teardown(
        route_id: &str,
        _self_peer: &String,
        destination_routes: Arc<Mutex<HashMap<String, String>>>,
        known_routes: Arc<Mutex<HashMap<String, bool>>>,
        logger: &Logger,
    ) -> Result<RouteMessage, MeshSimError> {
        //Clean the destination routes cache
        {
            let mut dr = destination_routes
                .lock()
                .expect("Failed to lock destination_routes table");
            let mut entry: Option<String> = None;
            for (dest, route) in dr.iter() {
                if route == route_id {
                    entry = Some(dest.to_owned());
                }
            }

            match entry {
                Some(dest) => {
                    let _res = dr.remove(&dest);
                    info!(logger, "Route to {} removed from cache", dest);
                }
                None => {
                    warn!(
                        logger,
                        "Route {} not associated to any destination", route_id
                    );
                }
            }
        }
        //Clean the known-routes cache
        {
            let mut kr = known_routes
                .lock()
                .expect("Failed to lock known_routes table");
            match kr.remove(route_id) {
                Some(_) => info!(logger, "Route {} removed from cache", route_id),
                None => warn!(logger, "Not subscribed to route {}", route_id),
            }
        }

        let msg = RouteMessage {
            route_source: String::new(),
            route_destination: String::new(),
            route_id: route_id.to_owned(),
            route: vec![],
        };
        // let hdr = MessageHeader::new(
        //     self_peer.clone(),
        //     String::new(),
        //     serialize_message(Messages::RouteTeardown(msg))?,
        // );

        Ok(msg)
    }

    fn update_queued_transmissions(
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        destination: &str,
        route_id: String,
    ) -> Result<(), MeshSimError> {
        let mut qt = queued_transmissions
            .lock()
            .expect("Failed to lock queued_transmissions");
        let mut new_flows = Vec::new();
        if qt.contains_key(destination) {
            let entry = qt.entry(destination.to_owned()).or_insert(vec![]);
            for h in entry.iter_mut() {
                if let Messages::Data(mut msg) = deserialize_message(h.get_payload())? {
                    msg.route_id = route_id.clone();
                    h.payload = serialize_message(Messages::Data(msg))?;
                    new_flows.push(h.clone());
                }
            }
            entry.clear();
            entry.append(&mut new_flows);
        }

        Ok(())
    }
}

fn deserialize_message(data: &[u8]) -> Result<Messages, MeshSimError> {
    bincode::deserialize(data).map_err(|e| {
        let err_msg = String::from("Error deserializing data into message");
        MeshSimError {
            kind: MeshSimErrorKind::Serialization(err_msg),
            cause: Some(Box::new(e)),
        }
    })
}

fn serialize_message(msg: Messages) -> Result<Vec<u8>, MeshSimError> {
    bincode::serialize(&msg).map_err(|e| {
        let err_msg = String::from("Error serializing message");
        MeshSimError {
            kind: MeshSimErrorKind::Serialization(err_msg),
            cause: Some(Box::new(e)),
        }
    })
}

#[cfg(test)]
mod tests {

    #[derive(Debug, Serialize)]
    struct SampleStruct;

    #[ignore]
    #[test]
    fn test_simple_three_hop_route_discovery() {
        unimplemented!();
    }
}
