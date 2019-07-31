//! This module implements the Reactive Gossip routing protocol
extern crate serde_cbor;
extern crate rand;
extern crate md5;

use crate::worker::protocols::Protocol;
use crate::worker::{WorkerError, Peer, MessageHeader};
use crate::worker::radio::*;
use std::sync::{Arc, Mutex};
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;
use self::rand::{rngs::StdRng, Rng};
use self::md5::Digest;
use crate::worker::rand::prelude::*;
use ::slog::Logger;

use std::collections::{HashMap, HashSet};
use std::thread;
use std::time::Duration;

//TODO: Parameterize these
const DEFAULT_MIN_HOPS : usize = 1;
const DEFAULT_GOSSIP_PROB : f64 = 0.70;
const MSG_CACHE_SIZE : usize = 200;
const CONCCURENT_THREADS_PER_FLOW : usize = 2;
const MSG_TRANSMISSION_THRESHOLD : u64 = 1000;

/// Main structure used in this protocol
#[derive(Debug)]
pub struct ReactiveGossipRoutingII {
    /// Configuration parameter for the protocol that indicates the minimum number of hops a message
    /// has to traverse before applying the gossip probability.
    k : usize,
    /// Gossip probability per node
    p : f64,
    worker_name : String,
    worker_id : String,
    short_radio : Arc<Radio>,
    /// Used for rapid decision-making of whether to forward a data message or not.
    known_routes : Arc<Mutex<HashMap<String, bool>>>,
    /// Destinations for which a route-discovery process has started but not yet finished.
    pending_destinations : Arc<Mutex<HashSet<String>>>,
    /// Used to cache recent route-discovery messages received.
    route_msg_cache : Arc<Mutex<HashSet<String>>>,
    /// Used to cache recent data messaged received.
    data_msg_cache : Arc<Mutex<HashMap<String, DataCacheEntry>>>,
    /// The map is indexed by destination (worker_name) and the value is the associated
    /// route_id for that route.
    destination_routes : Arc<Mutex<HashMap<String, String>>>,
    /// Map to keep track of pending transmissions that are waiting for their associated route_id to be
    /// established.
    queued_transmissions : Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
    /// RNG used for routing calculations
    rng : StdRng, //TODO: MUST USE THIS RNG!
    /// The logger to use in this protocol
    logger : Logger,
}

/// This struct contains the state necessary for the protocol to discover routes between
/// two nodes and if one or more exist, establish said routes.
#[derive(Debug, Serialize, Deserialize)]
struct RouteMessage {
    pub route_id : String,
    pub route : Vec<String>,
}

impl RouteMessage {
    fn new(source : String, destination : String) -> RouteMessage {
        let mut data = Vec::new();
        data.append(&mut source.clone().into_bytes());
        data.append(&mut destination.clone().into_bytes());
        let dig = md5::compute(&data);
        let route_id = format!("{:x}", dig);

        RouteMessage{ route_id : route_id, 
                      route :  vec![] }
    }
}

///This message uses an already established route to send data to the destination
#[derive(Debug, Serialize, Deserialize)]
struct DataMessage { 
    pub payload : Vec<u8>,
    pub route_id : String,
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
    state : DataMessageStates,
    data : Option<MessageHeader>,

}

/// Enum that lists all the possible messages in this protocol as well as the associated data for each one
#[derive(Debug, Serialize, Deserialize)]
enum Messages { 
    RouteDiscovery(RouteMessage),
    RouteEstablish(RouteMessage),
    RouteTeardown(RouteMessage),
    Data(DataMessage),
}

impl Protocol for ReactiveGossipRoutingII {
    fn handle_message(&self,  mut hdr : MessageHeader, _r_type : RadioTypes) -> Result<Option<MessageHeader>, WorkerError> {        
        let msg_hash = hdr.get_hdr_hash()?;

        let data = match hdr.payload.take() {
            Some(d) => { d },
            None => {
                warn!(self.logger, "Messaged received from {:?} had empty payload.", hdr.sender);
                return Ok(None)
            }
        };

        let msg = ReactiveGossipRoutingII::build_protocol_message(data)?;
        let queued_transmissions = Arc::clone(&self.queued_transmissions);
        let dest_routes = Arc::clone(&self.destination_routes);
        let pending_destinations = Arc::clone(&self.pending_destinations);
        let known_routes = Arc::clone(&self.known_routes);
        let self_peer = self.get_self_peer();
        let short_radio = Arc::clone(&self.short_radio);
        let route_msg_cache = Arc::clone(&self.route_msg_cache);
        let data_msg_cache = Arc::clone(&self.data_msg_cache);
        ReactiveGossipRoutingII::handle_message_internal(hdr, msg, self_peer, msg_hash, self.k, self.p,
                                                       queued_transmissions, dest_routes,
                                                       pending_destinations,
                                                       known_routes, route_msg_cache,
                                                       data_msg_cache, short_radio,
                                                       &self.logger)
    }

    fn init_protocol(&self) -> Result<Option<MessageHeader>, WorkerError> {
        let logger = self.logger.clone();
        let radio = Arc::clone(&self.short_radio);
        let data_msg_cache = Arc::clone(&self.data_msg_cache);
        let dest_routes = Arc::clone(&self.destination_routes);
        let known_routes = Arc::clone(&self.known_routes);
        let self_peer = self.get_self_peer();
        let _handle = thread::spawn(move || {
            info!(logger, "Retransmission loop started");
            ReactiveGossipRoutingII::retransmission_loop(data_msg_cache,
                                                       dest_routes,
                                                       known_routes,
                                                       radio,
                                                       self_peer,
                                                       logger);
        });

        Ok(None)
    }

    fn send(&self, destination : String, data : Vec<u8>) -> Result<(), WorkerError> {
        let routes = self.destination_routes.lock()?;

        //Check if an available route exists for the destination.
        if let Some(route_id) = routes.get(&destination) {
            //If one exists, start a new flow with the route
            let _res = ReactiveGossipRoutingII::start_flow(route_id.to_string(),
                                                         destination, 
                                                         self.get_self_peer(), 
                                                         data,
                                                         Arc::clone(&self.short_radio),
                                                         Arc::clone(&self.data_msg_cache))?;
            info!(self.logger, "Data has been transmitted");
        } else {
            let mut pending_routes = self.pending_destinations.lock()?;
            let qt = Arc::clone(&self.queued_transmissions);
            let route_id = match pending_routes.insert(destination.clone()) {
                false => {
                    info!(self.logger, "Route discovery process already started for {}", &destination);
                    pending_routes.get(&destination).unwrap().to_owned()
                },
                true => {
                    info!(self.logger, "No known route to {}. Starting discovery process.", &destination);
                    //If no route exists, start a new route discovery process...
                    let route_id = self.start_route_discovery(destination.clone())?;
                    route_id
                },
            };
            //...and then queue the data transmission for when the route is established
            let _res = ReactiveGossipRoutingII::queue_transmission(qt, route_id, data)?;
        }
        Ok(())
    }
}

impl ReactiveGossipRoutingII {
    /// Creates a new instance of the protocol.
    pub fn new( worker_name : String,
                worker_id : String,
                short_radio : Arc<Radio>,
                rng : StdRng,
                logger : Logger ) -> ReactiveGossipRoutingII {
        let qt = HashMap::new();
        let d_routes = HashMap::new();
        let k_routes = HashMap::new();
        let route_cache = HashSet::new();
        let data_cache = HashMap::new();
        let pending_destinations = HashSet::new();
        ReactiveGossipRoutingII{ k : DEFAULT_MIN_HOPS,
                               p : DEFAULT_GOSSIP_PROB,
                               worker_name : worker_name,
                               worker_id : worker_id, 
                               short_radio : short_radio,
                               destination_routes : Arc::new(Mutex::new(d_routes)),
                               queued_transmissions : Arc::new(Mutex::new(qt)),
                               known_routes : Arc::new(Mutex::new(k_routes)),
                               pending_destinations : Arc::new(Mutex::new(pending_destinations)),
                               route_msg_cache : Arc::new(Mutex::new(route_cache)),
                               data_msg_cache : Arc::new(Mutex::new(data_cache)),
                               rng : rng,
                               logger : logger }
    }

    fn start_route_discovery(&self, destination : String) -> Result<String, WorkerError> {
        let mut msg = RouteMessage::new(self.worker_id.clone(), destination.clone());
        msg.route.push(self.worker_name.clone());
        let route_id = msg.route_id.clone();
        let mut hdr = MessageHeader::new();
        hdr.sender.id = self.worker_id.clone();
        hdr.sender.name = self.worker_name.clone();
        hdr.destination.name = destination;
        let payload = to_vec(&Messages::RouteDiscovery(msg))?;
        hdr.payload = Some(payload);
        
        let _res = self.short_radio.broadcast(hdr)?;
        info!(self.logger, "Route discovery process started for route_id {}", &route_id);

        Ok(route_id)
    }

    fn start_flow(route_id : String, dest : String, self_peer : Peer, data : Vec<u8>,
                  short_radio : Arc<Radio>,
                  data_msg_cache : Arc<Mutex<HashMap<String, DataCacheEntry>>>) -> Result<(), WorkerError> {
        let dest_peer = Peer { name : dest,
                               id : String::from(""),
                               short_address : None,
                               long_address : None  };
        let mut hdr = MessageHeader::new();
        hdr.sender = self_peer;
        hdr.destination = dest_peer;
        hdr.hops = 1;
        hdr.payload = Some(to_vec(&Messages::Data(DataMessage{ route_id : route_id.clone(),
                                                               payload  : data }))?);

        //Log this message in the data_msg_cache so that we can monitor if the neighbors relay it
        //properly, retransmit if necessary, and don't relay it again when we hear it from others.
        let mut dc = data_msg_cache.lock()?;
        let msg_hsh = format!("{:x}", &hdr.get_hdr_hash()?);
        dc.insert(msg_hsh, DataCacheEntry{ state : DataMessageStates::Pending(route_id.clone()),
                                                            data : Some(hdr.clone())});
        //Right now this assumes the data can be sent in a single broadcast message
        //This might be addressed later on.
        short_radio.broadcast(hdr)
    }

    fn queue_transmission(trans_q : Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
                          route_id : String, 
                          data : Vec<u8>) -> Result<(), WorkerError> {
        let mut tq = match trans_q.lock() {
            Ok(q) => q,
            Err(e) => return Err(WorkerError::Sync(format!("Error trying to acquire lock to transmissions queue: {}", e)))
        };
        let e = tq.entry(route_id).or_insert(vec![]);
        e.push(data);
        Ok(())
    }

    fn handle_message_internal(hdr : MessageHeader,
                               msg : Messages,
                               self_peer : Peer,
                               msg_hash : Digest,
                               k : usize,
                               p : f64,
                               queued_transmissions : Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>, 
                               dest_routes : Arc<Mutex<HashMap<String, String>>>,
                               pending_destinations : Arc<Mutex<HashSet<String>>>,
                               known_routes : Arc<Mutex<HashMap<String, bool>>>,
                               route_msg_cache : Arc<Mutex<HashSet<String>>>,
                               data_msg_cache : Arc<Mutex<HashMap<String, DataCacheEntry>>>,
                               short_radio : Arc<Radio>, logger : &Logger ) -> Result<Option<MessageHeader>, WorkerError> {
        let response = match msg {
                    Messages::Data(data_msg) => {
//                        debug!(logger, "Received DATA message");
                        ReactiveGossipRoutingII::process_data_msg( hdr, data_msg, known_routes, data_msg_cache,
                                                                 self_peer, msg_hash, logger,)
                    },
                    Messages::RouteDiscovery(route_msg) => {
//                        debug!(logger, "Received ROUTE_DISCOVERY message");
                        ReactiveGossipRoutingII::process_route_discovery_msg(hdr, route_msg,
                                                                           k, p, known_routes,
                                                                           route_msg_cache, self_peer, msg_hash,
                                                                           logger, )
                    },
                    Messages::RouteEstablish(route_msg) => {
//                        debug!(logger, "Received ROUTE_ESTABLISH message");
                        ReactiveGossipRoutingII::process_route_established_msg(hdr,
                                                                             route_msg,
                                                                             known_routes,
                                                                             dest_routes,
                                                                             pending_destinations,
                                                                             queued_transmissions,
                                                                             data_msg_cache,
                                                                             self_peer,
                                                                             short_radio, logger)
                    },
                    Messages::RouteTeardown(route_msg) => {
                        ReactiveGossipRoutingII::process_route_teardown_msg(hdr,
                                                                        route_msg,
                                                                        known_routes,
                                                                        dest_routes,
                                                                        pending_destinations,
                                                                        queued_transmissions,
                                                                        data_msg_cache,
                                                                        self_peer,
                                                                        short_radio, logger)

                    }
                };
        response
    }

    fn process_data_msg(mut hdr : MessageHeader, 
                            data_msg : DataMessage, 
                            known_routes : Arc<Mutex<HashMap<String, bool>>>,
                            data_msg_cache : Arc<Mutex<HashMap<String, DataCacheEntry>>>,
                            self_peer : Peer,
                            msg_hash : Digest,
                            logger : &Logger) -> Result<Option<MessageHeader>, WorkerError> {
        info!(logger, "Received DATA message {:x} from {}", &msg_hash, &hdr.sender.name);
        let route_id = data_msg.route_id.clone();

        {
            let part_of_route = {
                let known_routes = known_routes.lock()?;
                known_routes.get(&data_msg.route_id).copied()
            };

            //Are we part of this route?
            let last_hop = match part_of_route {
                Some(last_hop) => last_hop,
                None => {
                    info!(logger, "Not part of this route. Dropping");
                    return Ok(None)
                },
            };

            //We are part of the route then.
            //Increase hop count...
            hdr.hops += 1;
            //...and re-package the message.
            hdr.payload = Some(to_vec(&Messages::Data(data_msg))?);

            let mut d_cache = data_msg_cache.lock()?;
            //Is this a new message?
            if let Some((msg, mut entry)) = d_cache.remove_entry(&format!("{:x}", &msg_hash)) {
                match entry.state {
                    DataMessageStates::Pending(_route_id) => {
                        entry.state = DataMessageStates::Confirmed;
                        let unused_data = entry.data.take();
                        drop(unused_data);
                        info!(logger, "{:x} marked as confirmed", &msg_hash);
                    },
                    DataMessageStates::Confirmed => {
                        info!(logger, "Dropping duplicate message {:x}", &msg_hash);
                    },
                }
                d_cache.insert(msg, entry);
                return Ok(None)
            } else {
                //This is a new message
                //Is there space in the cache?
                if d_cache.len() >= MSG_CACHE_SIZE {
                    let e : String = d_cache.keys()
                        .take(1) //Select the first random element to remove.
                        .next() //Take it.
                        .unwrap()
                        .to_owned(); //Copy the data so we can get a mutable borrow later.
                    d_cache.remove(&e);
                }
                let state = match last_hop {
                    true => DataMessageStates::Confirmed,
                    false => DataMessageStates::Pending(route_id),
                };
                d_cache.insert(format!("{:x}", &msg_hash), DataCacheEntry{ state : state,
                                                                                 data : Some(hdr.clone())});
            }
        }

        //Are the intended recipient?
        if hdr.destination.name == self_peer.name {
            info!(logger, "Message {:x} has reached its destination", msg_hash; "route_length" => hdr.hops);
            return Ok(None)
        } else {
            //We are not. Forward the message.
            return Ok(Some(hdr))
        }
    }

    fn process_route_discovery_msg( mut hdr : MessageHeader, 
                                    mut msg : RouteMessage,
                                    k : usize, p : f64,
                                    known_routes : Arc<Mutex<HashMap<String, bool>>>,
                                    route_msg_cache : Arc<Mutex<HashSet<String>>>,
                                    self_peer : Peer,
                                    _msg_hash : Digest,
                                    logger : &Logger ) -> Result<Option<MessageHeader>, WorkerError> {
        info!(logger, "Received ROUTE_DISCOVERY message"; "route_id" => &msg.route_id, "source" => &hdr.sender.name);

        //Is this node the intended destination of the route?
        if hdr.destination.name == self_peer.name {
            info!(logger, "Route discovery succeeded! Route_id {}", &msg.route_id);

            //Add this route to the list of known routes.
            //This is now needed in the current version of the protocol as the process_data_msg
            //function discriminates on route-membership first of all. Also, must add this route as
            //"last-hop" even though technically it isn't, since we dont need to confirm the data
            //messages that come through it, since it won't be relaying them (this is the destination).
            {
                let mut kr = known_routes.lock()?;
                kr.insert(msg.route_id.clone(), true);
            }

            //Update route
            msg.route.insert(0, self_peer.name.clone());
            //Update the header
            hdr.destination = hdr.sender.clone();
            hdr.sender = self_peer;
            hdr.payload = Some(to_vec(&Messages::RouteEstablish(msg))?);
            return Ok(Some(hdr))
        }

        // //Is this node already active in this route? e.g. overhearing the broadcast of a neighbour that received
        // //the message from this node in the first place.
        // if msg.route.contains(&self_peer.name) {
        //     //We are already part of this route
        //     debug!("Already part of this route. Dropping message");
        //     return Ok(None)
        // }

        //Is this a new route?
        {
            let mut p_routes = route_msg_cache.lock()?;
            if !p_routes.insert(msg.route_id.clone()) {
                //We have processed this route before. Discard message
                info!(logger, "Route {} has already been processed. Dropping message", &msg.route_id);
                return Ok(None)
            }
        }

        //Gossip?
        let mut rng = thread_rng();
        let s : f64 = rng.gen_range(0f64, 1f64);
        debug!(logger, "Gossip prob {}", s);
        if msg.route.len() > k && s > p {
            info!(logger, "Not forwarding the message");
            //Not gossiping this message.
            return Ok(None)
        }
        
        //Update route
        msg.route.push(self_peer.name.clone());
        info!(logger, "{} added to route_id {}", &self_peer.name, msg.route_id);

        //Build message and forward it
        hdr.payload = Some(to_vec(&Messages::RouteDiscovery(msg))?);
        Ok(Some(hdr))
    }

    fn process_route_established_msg(mut hdr : MessageHeader, 
                                     mut msg : RouteMessage, 
                                     known_routes : Arc<Mutex<HashMap<String, bool>>>,
                                     dest_routes : Arc<Mutex<HashMap<String, String>>>,
                                     pending_destinations : Arc<Mutex<HashSet<String>>>,
                                     queued_transmissions : Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
                                     data_msg_cache : Arc<Mutex<HashMap<String, DataCacheEntry>>>,
                                     self_peer : Peer,
                                     short_radio : Arc<Radio>,
                                     logger : &Logger) -> Result<Option<MessageHeader>, WorkerError> {
        info!(logger, "Received ROUTE_ESTABLISH message"; "route_id" => &msg.route_id, "source" => &hdr.sender.name);

        //Who's the next hop in the route?
        let next_hop = match msg.route.pop() {
            Some(h) => h,
            None => {
                //The route in this message is empty. Discard the message.
                warn!(logger, "Received empty route message for route_id {}", &msg.route_id);
                return Ok(None)
            }
        };
        //Is the message meant for this node?
        if next_hop != self_peer.name {
            //Not us. Discard message
            return Ok(None)
        }

        //Add the this route to the known_routes list...
        {
            let mut kr = known_routes.lock()?;
            //Indicate whether this node is the last hop in the route to the destination.
            //This will be used by the node to know whether it has to verify packets it forwards,
            //since th destination will not relay them.
            let last_hop = hdr.sender.name == msg.route[0];
            let _res = kr.insert(msg.route_id.clone(), last_hop);
        }
        //...and the destination-route list
        {
            let mut dr = dest_routes.lock()?;
            let _res = dr.insert(hdr.sender.name.clone(), msg.route_id.clone());
        }

        //This is the next hop in the route. Update route to reflect this.
        msg.route.insert(0, next_hop);

        // DEBUG
        info!(logger, "Known_routes: {:?}", &known_routes);
        info!(logger, "Destination_routes: {:?}", &dest_routes);

        //Is this the final destination of the route?
        if hdr.destination.name == self_peer.name {
            info!(logger, "Route establish succeeded! Route_id: {}", &msg.route_id);
            info!(logger, "Route: {:?}", &msg.route);

            //...and remove this node from the pending destinations
            {
                let mut pd = pending_destinations.lock()?;
                let _res = pd.remove(&hdr.sender.name);
            }

            //Start any flows that were waiting on the route
            let _res = ReactiveGossipRoutingII::start_queued_flows(queued_transmissions,
                                                                 msg.route_id.clone(),
                                                                 hdr.sender.name.clone(),
                                                                 self_peer,
                                                                 short_radio,
                                                                 data_msg_cache,
                                                                 logger);
            return Ok(None)
        }

        //Finally, forward the message
        hdr.payload = Some(to_vec(&Messages::RouteEstablish(msg))?);
        Ok(Some(hdr))
    }

    fn process_route_teardown_msg(_hdr : MessageHeader,
                                     msg : RouteMessage,
                                     known_routes : Arc<Mutex<HashMap<String, bool>>>,
                                     dest_routes : Arc<Mutex<HashMap<String, String>>>,
                                     _pending_destinations : Arc<Mutex<HashSet<String>>>,
                                     _queued_transmissions : Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
                                     _data_msg_cache : Arc<Mutex<HashMap<String, DataCacheEntry>>>,
                                     self_peer : Peer,
                                     _short_radio : Arc<Radio>,
                                     logger : &Logger) -> Result<Option<MessageHeader>, WorkerError> {
        info!(logger, "Route TEARDOWN msg received for route {}", &msg.route_id);

        let subscribed = {
            let kr = known_routes.lock()?;
            kr.contains_key(&msg.route_id)
        };

        let response = match subscribed {
            true => {
                let hdr = ReactiveGossipRoutingII::route_teardown(&msg.route_id,
                                                  &self_peer,
                                                  dest_routes,
                                                  known_routes,
                                                  logger)?;
                Some(hdr)
            },
            false => {
                info!(logger, "Not subscribed to route. Ignoring");
                None
            }
        };
        Ok(response)
    }

    fn start_queued_flows(queued_transmissions : Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
                          route_id : String,
                          destination : String,
                          self_peer : Peer,
                          short_radio : Arc<Radio>,
                          data_msg_cache : Arc<Mutex<HashMap<String, DataCacheEntry>>>,
                          logger : &Logger ) ->Result<(), WorkerError> {
        let mut entry : Option<(String, Vec<Vec<u8>>)> = None;

        info!(logger, "Looking for queued flows for route_id {}", &route_id );

        //Obtain the lock
        {
            let mut qt = queued_transmissions.lock()?;
            entry = qt.remove_entry(&route_id);
        }
        //Lock released

        let thread_pool = threadpool::Builder::new().num_threads(CONCCURENT_THREADS_PER_FLOW).build();
        if let Some((_key, flows)) = entry {
            info!(logger, "Processing {} queued transmissions.", &flows.len());
            for data in flows {
                let r_id = route_id.clone();
                let dest = destination.clone();
                let s = self_peer.clone();
                let radio = Arc::clone(&short_radio);
                let l = logger.clone();
                let dmc = Arc::clone(&data_msg_cache);
                thread_pool.execute(move || {
                    match ReactiveGossipRoutingII::start_flow(r_id.clone(),
                                                            dest,
                                                            s,
                                                            data,
                                                            radio,
                                                            dmc) {
                        Ok(_) => {
                            // All good!
                        },
                        Err(e) => {
                            error!(l, "Failed to start transmission for route {} - {}", r_id, e);
                        },
                    }
                });
            }
        }

        //Wait for all threads to finish
        thread_pool.join();
        Ok(())
    }

    fn build_protocol_message(data : Vec<u8>) -> Result<Messages, serde_cbor::Error> {
        let res : Result<Messages, serde_cbor::Error> = from_slice(data.as_slice());
        res
    }

    fn get_self_peer(&self) -> Peer {
        Peer{ name : self.worker_name.clone(),
              id : self.worker_id.clone(),
              short_address : None,
              long_address : None, }
    }

    fn retransmission_loop( data_msg_cache : Arc<Mutex<HashMap<String, DataCacheEntry>>>,
                            destination_routes : Arc<Mutex<HashMap<String, String>>>,
                            known_routes : Arc<Mutex<HashMap<String, bool>>>,
                            short_radio : Arc<Radio>,
                            self_peer : Peer,
                            logger : Logger) -> Result<(), WorkerError> {
        let sleep_time = Duration::from_millis(MSG_TRANSMISSION_THRESHOLD);

        loop {
            thread::sleep(sleep_time);
            let mut cache = data_msg_cache.lock()?;
            for (msg_hash, entry) in cache.iter_mut()
                .filter(|(_msg_hash, entry)| entry.state.is_pending()) {
                    debug!(logger, "Message {} is pending confirmation", &msg_hash);

                    if let Some(hdr) = entry.data.take() {
                        info!(logger, "Retransmitting message {:x}", &hdr.get_hdr_hash()?);

                        //The message is still cached, so re-transmit it.
                        short_radio.broadcast(hdr).map_err( |e| {
                            error!(logger, "Failed to re-transmit message. {}", e);
                        });
                    } else {
                        // The message is no longer cached.
                        // At this point, we assume the route is broken.
                        // Send the route teardown message.
                        if let DataMessageStates::Pending(ref route_id) = entry.state.clone() {
                            info!(logger, "Starting route {} teardown", route_id);

                            //Mark this entry in the message cache as confirmed so that we don't
                            //process it again.
                            entry.state = DataMessageStates::Confirmed;

                            //Tear down the route in the internal caches
                            let hdr = ReactiveGossipRoutingII::route_teardown(route_id,
                                                                                          &self_peer,
                                                                                          Arc::clone(&destination_routes),
                                                                                          Arc::clone(&known_routes),
                                                                                          &logger, )?;
                            //Send the teardown message
                            short_radio.broadcast(hdr).map_err( |e| {
                                error!(logger, "Failed to send route-teardown message. {}", e);
                            });
                        } else {
                            error!(logger, "Inconsistent state reached");
                            unreachable!();
                        }
                    }
                }
        }
        Ok(())
    }

    fn route_teardown(route_id : &String,
                      self_peer : &Peer,
                      destination_routes : Arc<Mutex<HashMap<String, String>>>,
                      known_routes : Arc<Mutex<HashMap<String, bool>>>,
                      logger : &Logger) -> Result<MessageHeader, WorkerError> {
        //Clean the destination routes cache
        {
            let mut dr = destination_routes.lock()?;
            let mut entry : Option<String> = None;
            for (dest, route) in dr.iter() {
                if route == route_id {
                    entry = Some(dest.to_owned());
                }
            }

            match entry {
                Some(dest) => {
                    let _res = dr.remove(&dest);
                    info!(logger, "Route to {} removed from cache", dest);
                },
                None => {
                    warn!(logger, "Route {} not associated to any destination", route_id);
                },
            }
        }

        //Clean the known-routes cache
        {
            let mut kr = known_routes.lock()?;
            match kr.remove(route_id) {
                Some(_) => info!(logger, "Route {} removed from cache", route_id),
                None => warn!(logger, "Not subscribed to route {}", route_id),
            }
        }

        let mut hdr = MessageHeader::new();
        hdr.sender = self_peer.clone();
        let msg = RouteMessage{ route_id : route_id.to_owned(), route : vec![] };
        hdr.payload = Some(to_vec(&Messages::RouteTeardown(msg))?);

        Ok(hdr)
    }
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
