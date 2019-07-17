//! This module implements the Reactive Gossip routing protocol
extern crate serde_cbor;
extern crate rand;
extern crate md5;

use worker::protocols::Protocol;
use worker::{WorkerError, Peer, MessageHeader};
use worker::radio::*;
use std::sync::{Arc, Mutex};
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;
use self::rand::{StdRng, Rng};
use self::md5::Digest;
use worker::rand::prelude::*;
use ::slog::Logger;

use std::collections::{HashMap, HashSet};
use std::thread;

//TODO: Parameterize these
const DEFAULT_MIN_HOPS : usize = 1;
const DEFAULT_GOSSIP_PROB : f64 = 0.70;
const MSG_CACHE_SIZE : usize = 200;
const CONCCURENT_THREADS_PER_FLOW : usize = 2;

/// Main structure used in this protocol
#[derive(Debug)]
pub struct ReactiveGossipRouting {
    /// Configuration parameter for the protocol that indicates the minimum number of hops a message
    /// has to traverse before applying the gossip probability.
    k : usize,
    /// Gossip probability per node
    p : f64,
    worker_name : String,
    worker_id : String,
    short_radio : Arc<Radio>,
    /// Used for rapid decision-making of whether to forward a data message or not.
    known_routes : Arc<Mutex<HashSet<String>>>,
    /// Destinations for which a route-discovery process has started but not yet finished.
    pending_destinations : Arc<Mutex<HashSet<String>>>,
    /// Used to cache recent route-discovery messages received.
    route_disc_cache : Arc<Mutex<HashSet<String>>>,
    /// Used to cache recent data messaged received.
    data_msg_cache : Arc<Mutex<HashSet<String>>>,
    /// The map is indexed by destination (worker_name) and the value is the associated
    /// route_id for that route.
    destination_routes : Arc<Mutex<HashMap<String, String>>>,
    /// Map to keep track of pending transmissions that are waiting for their associated route_id to be
    /// established.
    queued_transmissions : Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
    /// RNG used for routing calculations
    rng : StdRng,
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

/// Enum that lists all the possible messages in this protocol as well as the associated data for each one
#[derive(Debug, Serialize, Deserialize)]
enum Messages { 
    RouteDiscovery(RouteMessage),
    RouteEstablish(RouteMessage),
    Data(DataMessage),
}

impl Protocol for ReactiveGossipRouting {
    fn handle_message(&self,  mut hdr : MessageHeader, _r_type : RadioTypes) -> Result<Option<MessageHeader>, WorkerError> {        
        let msg_hash = hdr.get_hdr_hash()?;

        let data = match hdr.payload.take() {
            Some(d) => { d },
            None => {
                warn!(self.logger, "Messaged received from {:?} had empty payload.", hdr.sender);
                return Ok(None)
            }
        };

        let msg = ReactiveGossipRouting::build_protocol_message(data)?;
        let queued_transmissions = Arc::clone(&self.queued_transmissions);
        let dest_routes = Arc::clone(&self.destination_routes);
        let pending_destinations = Arc::clone(&self.pending_destinations);
        let known_routes = Arc::clone(&self.known_routes);
        let self_peer = self.get_self_peer();
        let short_radio = Arc::clone(&self.short_radio);
        let route_disc_cache = Arc::clone(&self.route_disc_cache);
        let data_msg_cache = Arc::clone(&self.route_disc_cache);
        ReactiveGossipRouting::handle_message_internal(hdr, msg, self_peer, msg_hash, self.k, self.p,
                                                       queued_transmissions, dest_routes,
                                                       pending_destinations,
                                                       known_routes, route_disc_cache,
                                                       data_msg_cache, short_radio,
                                                       &self.logger)
    }

    fn init_protocol(&self) -> Result<Option<MessageHeader>, WorkerError> {
        //No initialization required for the protocol.
        Ok(None)
    }

    fn send(&self, destination : String, data : Vec<u8>) -> Result<(), WorkerError> {
        let routes = self.destination_routes.lock()?;

        //Check if an available route exists for the destination.
        if let Some(route_id) = routes.get(&destination) {
            //If one exists, start a new flow with the route
            let _res = ReactiveGossipRouting::start_flow(route_id.to_string(), 
                                                         destination, 
                                                         self.get_self_peer(), 
                                                         data,
                                                         Arc::clone(&self.short_radio))?;
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
            let _res = ReactiveGossipRouting::queue_transmission(qt, route_id, data)?;
        }
        Ok(())
    }
}

impl ReactiveGossipRouting {
    /// Creates a new instance of the protocol.
    pub fn new( worker_name : String,
                worker_id : String,
                short_radio : Arc<Radio>,
                rng : StdRng,
                logger : Logger ) -> ReactiveGossipRouting {
        let qt = HashMap::new();
        let d_routes = HashMap::new();
        let k_routes = HashSet::new();
        let route_cache = HashSet::new();
        let data_cache = HashSet::new();
        let pending_destinations = HashSet::new();
        ReactiveGossipRouting{ k : DEFAULT_MIN_HOPS,
                               p : DEFAULT_GOSSIP_PROB,
                               worker_name : worker_name,
                               worker_id : worker_id, 
                               short_radio : short_radio,
                               destination_routes : Arc::new(Mutex::new(d_routes)),
                               queued_transmissions : Arc::new(Mutex::new(qt)),
                               known_routes : Arc::new(Mutex::new(k_routes)),
                               pending_destinations : Arc::new(Mutex::new(pending_destinations)),
                               route_disc_cache : Arc::new(Mutex::new(route_cache)),
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
                  short_radio : Arc<Radio>) -> Result<(), WorkerError> {
        let dest_peer = Peer { name : dest,
                               id : String::from(""),
                               short_address : None,
                               long_address : None  };
        let mut hdr = MessageHeader::new();
        hdr.sender = self_peer;
        hdr.destination = dest_peer;
        hdr.hops = 1;
        hdr.payload = Some(to_vec(&Messages::Data(DataMessage{ route_id : route_id,
                                                               payload  : data }))?);

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
                               known_routes : Arc<Mutex<HashSet<String>>>,
                               route_disc_cache : Arc<Mutex<HashSet<String>>>,
                               data_msg_cache : Arc<Mutex<HashSet<String>>>,
                               short_radio : Arc<Radio>, logger : &Logger ) -> Result<Option<MessageHeader>, WorkerError> {
        let response = match msg {
                    Messages::Data(data_msg) => {
//                        debug!(logger, "Received DATA message");
                        ReactiveGossipRouting::process_data_msg( hdr, data_msg, known_routes, data_msg_cache,
                                                                 self_peer, msg_hash, logger,)
                    },
                    Messages::RouteDiscovery(route_msg) => {
//                        debug!(logger, "Received ROUTE_DISCOVERY message");
                        ReactiveGossipRouting::process_route_discovery_msg(hdr, route_msg,
                                                                           k, p, known_routes,
                                                                           route_disc_cache, self_peer, msg_hash,
                                                                           logger, )
                    },
                    Messages::RouteEstablish(route_msg) => {
//                        debug!(logger, "Received ROUTE_ESTABLISH message");
                        ReactiveGossipRouting::process_route_established_msg(hdr,
                                                                             route_msg,
                                                                             known_routes,
                                                                             dest_routes,
                                                                             pending_destinations,
                                                                             queued_transmissions,
                                                                             self_peer,
                                                                             short_radio, logger)
                    },
                };
        response
    }

    fn process_data_msg(mut hdr : MessageHeader, 
                            data_msg : DataMessage, 
                            known_routes : Arc<Mutex<HashSet<String>>>,
                            data_msg_cache : Arc<Mutex<HashSet<String>>>,
                            self_peer : Peer,
                            msg_hash : Digest,
                            logger : &Logger) -> Result<Option<MessageHeader>, WorkerError> {
        info!(logger, "Received DATA message {:x} from {}", &msg_hash, &hdr.sender.name);
        //Is this a new message?
        {
            let mut d_cache = data_msg_cache.lock()?;
            if d_cache.contains(&format!("{:x}", &msg_hash)) {
                info!(logger, "Dropping duplicate message {:x}", &msg_hash);
                return Ok(None)
            }
            //Is there space in the cache?
            if d_cache.len() >= MSG_CACHE_SIZE {
                let e : String = d_cache.iter()
                                        .take(1) //Select the first random element to remove.
                                        .next() //Take it.
                                        .unwrap()
                                        .to_owned(); //Copy the data so we can get a mutable borrow later.
                d_cache.remove(&e);
            }
            d_cache.insert(format!("{:x}", &msg_hash));
        }

        //Are the intended recipient?
        // debug!("Msg hdr: {:?}", &hdr);
        if hdr.destination.name == self_peer.name {
            info!(logger, "Message {:x} has reached it's destination", msg_hash; "route_length" => hdr.hops);
            return Ok(None)
        }

        //Are we part of this route?
        let rl = known_routes.lock()?;
        if !rl.contains(&data_msg.route_id) {
            info!(logger, "Not part of this route. Dropping");
            return Ok(None)
        }

        //We are part of the route then. Forward the message.
        hdr.payload = Some(to_vec(&Messages::Data(data_msg))?);
        //Increase hop count
        hdr.hops += 1;
        Ok(Some(hdr))
    }

    fn process_route_discovery_msg( mut hdr : MessageHeader, 
                                    mut msg : RouteMessage,
                                    k : usize, p : f64,
                                    known_routes : Arc<Mutex<HashSet<String>>>,
                                    route_disc_cache : Arc<Mutex<HashSet<String>>>,
                                    self_peer : Peer,
                                    msg_hash : Digest,
                                    logger : &Logger ) -> Result<Option<MessageHeader>, WorkerError> {
        info!(logger, "Received ROUTE_DISCOVERY message"; "route_id" => &msg.route_id, "source" => &hdr.sender.name);

        //Is this node the intended destination of the route?
        if hdr.destination.name == self_peer.name {
            info!(logger, "Route discovery succeeded! Route_id {}", &msg.route_id);
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
            let mut p_routes = route_disc_cache.lock()?;
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
                                     known_routes : Arc<Mutex<HashSet<String>>>,
                                     dest_routes : Arc<Mutex<HashMap<String, String>>>,
                                     pending_destinations : Arc<Mutex<HashSet<String>>>,
                                     queued_transmissions : Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>, 
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
        
        //This is the next hop in the route. Update route to reflect this.
        msg.route.insert(0, next_hop);

        //Add the this route to the known_routes list...
        {
            let mut kr = known_routes.lock()?;
            let _res = kr.insert(msg.route_id.clone());
        }
        //...and the destination-route list
        {
            let mut dr = dest_routes.lock()?;
            let _res = dr.insert(hdr.sender.name.clone(), msg.route_id.clone());
        }

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
            let _res = ReactiveGossipRouting::start_queued_flows(queued_transmissions,
                                                                 msg.route_id.clone(),
                                                                 hdr.sender.name.clone(),
                                                                 self_peer,
                                                                 short_radio,
                                                                 logger);
            return Ok(None)
        }

        //Finally, forward the message
        hdr.payload = Some(to_vec(&Messages::RouteEstablish(msg))?);
        Ok(Some(hdr))
    }

    fn start_queued_flows(queued_transmissions : Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
                          route_id : String,
                          destination : String,
                          self_peer : Peer,
                          short_radio : Arc<Radio>,
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
        if let Some((key, flows)) = entry {
            info!(logger, "Processing {} queued transmissions.", &flows.len());
            for data in flows {
                let r_id = route_id.clone();
                let dest = destination.clone();
                let s = self_peer.clone();
                let radio = Arc::clone(&short_radio);
                let l = logger.clone();
                thread_pool.execute(move || {
                    match ReactiveGossipRouting::start_flow(r_id.clone(), dest, s, data, radio) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[derive(Debug, Serialize)]
    struct SampleStruct;

    #[ignore]
    #[test]
    fn test_simple_three_hop_route_discovery() {
        unimplemented!();
    }

    //This test will me removed at some point, but it's useful at the moment for the message size calculations
    //that will be needed to do segementation of large pieces of data for transmission.
    #[ignore]
    #[test]
    fn test_message_size() {
        use std::mem;
        
        let mut self_peer = Peer::new();
        self_peer.name = String::from("Foo");
        self_peer.id = String::from("kajsndlkajsndaskdnlkjsadnks");

        let mut dest_peer = Peer::new();
        dest_peer.name = String::from("Bar");
        dest_peer.id = String::from("oija450njjcdlhbaslijdblahsd");

        let mut hdr = MessageHeader::new();
        hdr.sender = self_peer.clone();
        hdr.destination = dest_peer.clone();

        println!("Size of MessageHeader: {}", mem::size_of::<MessageHeader>());
        let hdr_size = mem::size_of_val(&hdr);
        println!("Size of a MessageHeader instance: {}", hdr_size);

        let mut msg = DataMessage{ route_id : String::from("SOME_ROUTE"),
                                   payload :  String::from("SOME DATA TO TRANSMIT").as_bytes().to_vec() };
        println!("Size of DataMessage: {}", mem::size_of::<DataMessage>());
        let msg_size = mem::size_of_val(&msg);
        println!("Size of a MessageHeader instance: {}", msg_size);
        let ser_msg = to_vec(&msg).expect("Could not serialize");
        println!("Size of ser_msg: {}", mem::size_of_val(&ser_msg));
        println!("Len of ser_msg: {}", ser_msg.len());
        let ser_hdr = to_vec(&hdr).expect("Could not serialize");
        println!("Len of ser_hdr: {}", ser_hdr.len());
        hdr.payload = Some(ser_msg);

        let final_hdr = to_vec(&hdr).expect("Could not serialize");
        println!("Len of final_hdr: {}", final_hdr.len());
        
        println!("Size of SampleStruct: {}", mem::size_of::<SampleStruct>());
        let s = SampleStruct;
        let xs = to_vec(&s).expect("Could not serialize");
        println!("Size of serialized SampleStruct: {}", xs.len());
        println!("xs: {:?}", &xs);
        
        let sample_data = [1u8; 2048];
        println!("sample data len: {}", sample_data.len());
        println!("sample data sizeof: {}", mem::size_of_val(&sample_data));
        let ser_sample_data = to_vec(&sample_data.to_vec()).expect("Could not serialize");
        println!("ser_sample_data len: {}", ser_sample_data.len());
        assert!(false);

    }
}
