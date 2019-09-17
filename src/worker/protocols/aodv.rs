//! Implemention of the AODV protocol, per its RFC https://www.rfc-editor.org/info/rfc3561
use crate::worker::protocols::Protocol;
use crate::worker::radio::*;
use crate::worker::{MessageHeader, Peer};
use crate::{MeshSimError, MeshSimErrorKind};

use md5::Digest;
use serde_cbor::de::*;
use serde_cbor::ser::*;
use slog::Logger;
use std::sync::{Arc, Mutex};
use chrono::{Utc, DateTime, Duration};
use chrono::offset::TimeZone;
use std::sync::atomic::{AtomicU32, Ordering};
use std::collections::{HashMap, HashSet};
use std::ops::Add;
use std::default::Default;
use rand::{rngs::StdRng, Rng};

// **************************************************
// ************ Configuration parameters ************
// **************************************************
const ACTIVE_ROUTE_TIMEOUT: u64 = 3000; //milliseconds
const ALLOWED_HELLO_LOSS: usize = 2;
const BLACKLIST_TIMEOUT: u64 = RREQ_RETRIES as u64 * NET_TRAVERSAL_TIME; //milliseconds
// const DELETE_PERIOD: 
const HELLO_INTERVAL: u64 = 1000; //milliseconds
const LOCAL_ADD_TTL: usize = 2;
const NET_DIAMETER: usize = 35;
// const MIN_REPAIR_TTL
const MY_ROUTE_TIMEOUT: u32 = 2 * ACTIVE_ROUTE_TIMEOUT as u32;
const NODE_TRAVERSAL_TIME: u64 = 40; //milliseconds
const NET_TRAVERSAL_TIME: u64 = 2 * NODE_TRAVERSAL_TIME * NET_DIAMETER as u64; //milliseconds
const NEXT_HOP_WAIT: u64 = NODE_TRAVERSAL_TIME + 10; //milliseconds
const PATH_DISCOVERY_TIME: u64 = 2 * NET_TRAVERSAL_TIME; //milliseconds
const RERR_RATELIMITE: usize = 10;
const RREQ_RETRIES: usize = 2;
const RREQ_RATELIMIT: usize = 10;
const TIMEOUT_BUFFER: u64 = 2; //in ??
const TTL_START: usize = 1;
const TTL_INCREMENT: usize = 2;
const TTL_THRESHOLD: usize = 7;
//I'm not actually sure this won't explode, given the precision of the numbers.
//Better to log the value and monitor it on tests.
lazy_static! {
    static ref MAX_REPAIR_TTL: usize = {
        let net = NET_DIAMETER as u16;
        let net = f64::from(net);
        let val = (0.3f64 * net).round();
        let val = val as u32;
        val as usize
    };
}
const fn get_ring_traversal_time(ttl : u64) -> u64 {
    // RING_TRAVERSAL_TIME: u64 = 2 * NODE_TRAVERSAL_TIME * (TTL_VALUE + TIMEOUT_BUFFER);
    let time : u64 = 2 * NODE_TRAVERSAL_TIME * (ttl + TIMEOUT_BUFFER);
    time
}

// **************************************************
// ***************** Main struct ********************
// **************************************************
bitflags! {
    #[derive(Default)]
    struct RTEFlags : u32 {
        const VALID_SEQ_NO = 0b00000001;
        const VALID_ROUTE = 0b00000010;
        const ACTIVE_ROUTE = 0b00000100;
    }
}

#[derive(Debug)]
struct RouteTableEntry {
    destination : String,
    dest_seq_no : u32,
    flags : RTEFlags,
    nic : String,
    next_hop: String,
    hop_count : u8,
    precursors : HashSet<String>,
    lifetime : DateTime<Utc>,
}

impl RouteTableEntry {
    fn new( 
        dest: &String,
        next_hop: &String,
        dest_seq_no : Option<u32>) -> Self {
        let mut flags = RTEFlags::VALID_SEQ_NO;
        let seq = match dest_seq_no {
            Some(seq) => {
                seq
            },
            None =>  { 
                flags = flags - RTEFlags::VALID_SEQ_NO;
                0
            },
        };
        RouteTableEntry{
            destination: dest.to_owned(),
            dest_seq_no: seq,
            flags,
            nic : String::from("DEFAULT"),
            next_hop: next_hop.to_owned(),
            hop_count: 0,
            precursors: HashSet::new(),
            lifetime: Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 0, 0),
        }
    }
}
/// Implementation of the Ad-hoc On-Demand Distance Vector routing protocol
#[derive(Debug)]
pub struct AODV {
    worker_name: String,
    worker_id: String,
    short_radio: Arc<dyn Radio>,
    sequence_number: Arc<AtomicU32>,
    rreq_seq_no: Arc<AtomicU32>,
    route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
    pending_routes: Arc<Mutex<HashMap<String, u32>>>,
    queued_transmissions: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
    rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
    rng: Arc<Mutex<StdRng>>,
    logger: Logger,
}

// **************************************************
// ******************  Messages *********************
// **************************************************
bitflags! {
    #[derive(Serialize, Deserialize, Default)]
    struct RREQFlags: u8 {
        const JOIN = 0b00000001;
        const REPAIR = 0b00000010;
        const GRATUITOUS_RREP = 0b00000100;
        const DESTINATION_ONLY = 0b00001000;
        const UNKNOWN_SEQUENCE_NUMBER = 0b00010000;
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RouteRequestMessage {
    flags: RREQFlags,
    hop_count: u8,
    rreq_id: u32,
    destination: String,
    dest_seq_no: u32,
    originator: String,
    orig_seq_no: u32,
}

bitflags! {
    #[derive(Serialize, Deserialize, Default)]
    struct RREPFlags: u8 {
        const REPAIR = 0b00000001;
        const ACK_REQUIRED = 0b00000010;
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RouteResponseMessage {
    flags: RREPFlags,
    //This field is not used in this implementation. Kept for completeness
    //in accordance to the RFC.
    prefix_size: u8, 
    hop_count: u8,
    destination: String,
    dest_seq_no: u32,
    originator: String,
    lifetime: u32,
}

bitflags! {
    #[derive(Serialize, Deserialize, Default)]
    struct RERRFlags: u8 {
        const NO_DELETE = 0b00000001;
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RouteErrorMessage {
    flags: RREPFlags,
    destinations: Vec<(String, u32)>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DataMessage {
    destination: String,
    payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
enum Messages {
    RREQ(RouteRequestMessage),
    RREP(RouteResponseMessage),
    REER(RouteErrorMessage),
    #[allow(non_camel_case_types)]
    RREP_ACK,
    DATA(DataMessage),
    HELLO,
}

// **************************************************
// **************** End Messages ********************
// **************************************************

impl Protocol for AODV {
    fn handle_message(
        &self,
        mut hdr: MessageHeader,
        r_type: RadioTypes,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
        let msg_hash = hdr.get_hdr_hash();

        let data = match hdr.payload.take() {
            Some(d) => d,
            None => {
                warn!(
                    self.logger,
                    "Messaged received from {:?} had empty payload.", hdr.sender
                );
                return Ok(None);
            }
        };

        let msg = deserialize_message(data)?;
        let route_table = Arc::clone(&self.route_table);
        let seq_no = Arc::clone(&self.sequence_number);
        let rreq_cache = Arc::clone(&self.rreq_cache);
        let rng = Arc::clone(&self.rng);
        AODV::handle_message_internal(
            hdr,
            msg,
            self.get_self_peer(),
            msg_hash,
            route_table,
            rreq_cache,
            seq_no,
            rng,
            &self.logger,
        )
    }

    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError> {
        info!(self.logger, "Protocol initialized");
        Ok(None)
    }

    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError> {
        let routes = Arc::clone(&self.route_table);
        let pending_routes = Arc::clone(&self.pending_routes);
        let routes = routes
            .lock()
            .expect("Failed to lock route_table");

        let mut dest_seq_no = 0;

        //Check if an available route exists for the destination.
        if let Some(route_entry) = routes.get(&destination) {
            //Is the route valid?
            if route_entry.flags.contains(RTEFlags::VALID_SEQ_NO) {
                //If one exists, start a new flow with the route
                AODV::start_flow(
                    destination,
                    self.get_self_peer(),
                    data,
                    Arc::clone(&self.short_radio),
                )?;
                info!(self.logger, "Data has been transmitted");
                return Ok(())
            }
            //Invalid route. Get the required data out of the entry and
            //fall through to route discovery.
            dest_seq_no = route_entry.dest_seq_no;
        }
        
        let mut pending_routes = pending_routes
            .lock()
            .expect("Failed to lock pending_destinations table");
        let qt = Arc::clone(&self.queued_transmissions);
        let route_id = match pending_routes.get(&destination) {
            Some(r_id) => { 
                info!(
                    self.logger,
                    "Route discovery process already started for {}", &destination
                );
                r_id.to_owned()
            },
            None => { 
                info!(
                    self.logger,
                    "No known route to {}. Starting discovery process.", &destination
                );
                self.start_route_discovery(destination.clone(), dest_seq_no)?
            },
        };

        //Add rreq to the cache so that this node does not re-process it
        let mut rr_cache = self.rreq_cache
            .lock()
            .expect("Error trying to acquire lock on rreq_cache");
        let cache_entry = rr_cache.insert((self.get_self_peer().name.clone(), route_id), Utc::now());

        //...and then queue the data transmission for when the route is established
        AODV::queue_transmission(qt, destination.clone(), data)?;
        
        Ok(())
    }
}

impl AODV {
    /// Instantiate a new handler for the AODV protocol
    pub fn new(
        worker_name: String,
        worker_id: String,
        short_radio: Arc<dyn Radio>,
        rng: Arc<Mutex<StdRng>>,
        logger: Logger,
    ) -> Self {
        let starting_rreq_id : u32 = { 
            let mut rng = rng.lock()
                        .expect("Could not lock RNG");
            rng.gen_range(0, std::u32::MAX) 
        };
        AODV {
            worker_name,
            worker_id,
            short_radio,
            sequence_number: Arc::new(AtomicU32::new(0)),
            rreq_seq_no: Arc::new(AtomicU32::new(starting_rreq_id)),
            route_table: Arc::new(Mutex::new(HashMap::new())),
            pending_routes: Arc::new(Mutex::new(HashMap::new())),
            queued_transmissions: Arc::new(Mutex::new(HashMap::new())),
            rreq_cache: Arc::new(Mutex::new(HashMap::new())),
            rng,
            logger,
        }
    }

    fn handle_message_internal(
        hdr: MessageHeader,
        msg: Messages,
        me: Peer,
        msg_hash: Digest,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        seq_no: Arc<AtomicU32>,
        // msg_cache: Arc<Mutex<Vec<CacheEntry>>>,
        rng: Arc<Mutex<StdRng>>,
        logger: &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
        match msg {
            Messages::DATA(msg) => {
                unimplemented!()
            },
            Messages::RREQ(msg) => {
                AODV::process_route_request_msg(
                    hdr,
                    msg,
                    route_table,
                    rreq_cache,
                    seq_no,
                    me,
                    logger,
                )
            },
            Messages::RREP(msg) => {
                AODV::process_route_response_msg(
                    hdr,
                    msg,
                    route_table,
                    rreq_cache,
                    seq_no,
                    me,
                    rng,
                    logger,
                )
            },
            Messages::REER(msg) => {
                unimplemented!()
            },
            Messages::RREP_ACK => {
                unimplemented!()
            },
            Messages::HELLO => {
                unimplemented!()
            }
        }
    }

    fn process_route_request_msg(
        mut hdr : MessageHeader,
        mut msg : RouteRequestMessage,
        route_table : Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        seq_no: Arc<AtomicU32>,
        me : Peer,
        logger : &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
        info!(logger, "RREQ message received from {}", &hdr.sender.name);

        //Create or update route to hdr.sender
        let mut rt = route_table
            .lock()
            .expect("Error trying to acquire lock on route table");
        let mut entry = rt.entry(hdr.sender.name.clone())
                            .or_insert_with(|| RouteTableEntry::new(&hdr.sender.name, &me.name, None));
        entry.lifetime = Utc::now().add(Duration::milliseconds(ACTIVE_ROUTE_TIMEOUT as i64));
        debug!(logger, "Route table entry: {:#?}", entry);

        //Check if this is a duplicate RREQ
        {
            let mut rr_cache = rreq_cache
                .lock()
                .expect("Error trying to acquire lock on rreq_cache");
            let cache_entry = rr_cache
                                .entry((msg.originator.clone(), msg.rreq_id))
                                .or_insert(Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 0, 0));
            let threshold_time = *cache_entry + Duration::milliseconds(PATH_DISCOVERY_TIME as i64);
            debug!(logger, "Threshold time: {:?}", &threshold_time);
            if  threshold_time > Utc::now() {
                //Duplicate RREQ. Drop.
                info!(logger, "Dropping duplicated RREQ message");
                *cache_entry = Utc::now();
                return Ok(None)
            }
            //Update the cache to the last time we received this RREQ
            *cache_entry = Utc::now();
        }

        //Increase msg hop_count
        msg.hop_count += 1;

        //Create/update route to msg.originator
        let mut entry = rt.entry(msg.originator.clone())
                            .or_insert_with(|| { 
            RouteTableEntry::new(&msg.originator, &hdr.sender.name, Some(msg.orig_seq_no)) 
        });
        entry.dest_seq_no = std::cmp::max(entry.dest_seq_no, msg.orig_seq_no);
        entry.flags.insert(RTEFlags::VALID_SEQ_NO); //Should this be done only if the seq_no is updated?
        entry.hop_count = msg.hop_count;
        let minimal_lifetime =  Utc::now() + 
                                Duration::milliseconds(2 * NET_TRAVERSAL_TIME as i64) -
                                Duration::milliseconds(2 * entry.hop_count as i64 * NODE_TRAVERSAL_TIME as i64);
        entry.lifetime = std::cmp::max(entry.lifetime, minimal_lifetime);

        debug!(logger, "Route table entry: {:#?}", entry);

        //This node generates an RREP if it is itself the destination
        if msg.destination == me.name {
            //RREQ succeeded.
            info!(logger, "RREQ reached its destination!");
            //If the message contains the curent seq_no, update it.
            let _old_seq = seq_no.compare_and_swap(msg.dest_seq_no, msg.dest_seq_no+1, Ordering::SeqCst);
            let flags : RREPFlags = Default::default();
            let response = RouteResponseMessage {
                flags: flags,
                prefix_size: 0, 
                hop_count: 0,
                destination: me.name.clone(),
                dest_seq_no: seq_no.load(Ordering::SeqCst),
                originator: msg.originator.clone(),
                lifetime: MY_ROUTE_TIMEOUT,
            };
            let mut resp_hdr = MessageHeader::new();
            let mut dest = Peer::new();
            dest.name = entry.next_hop.clone();
            resp_hdr.destination = dest;
            resp_hdr.sender = me.clone();
            resp_hdr.payload = Some(serialize_message(Messages::RREP(response))?);

            return Ok(Some(resp_hdr))
        }

        //or it has an active route to the destination... 
        if  let Some(entry) = rt.get_mut(&msg.destination) {
            // AND the dest_seq_no in the RT is greater than or equal to the one in the RREQ
            // AND also valid
            // AND the destination_only flag is not set
            // AND the route has not yet expired
            if  entry.dest_seq_no >= msg.dest_seq_no &&
                entry.flags.contains(RTEFlags::VALID_SEQ_NO) &&
                !msg.flags.contains(RREQFlags::DESTINATION_ONLY) &&
                entry.lifetime >= Utc::now() {
                    info!(logger, "A valid route to {} has been found!", &msg.destination);
                    
                    //Update forward-route entry's precursor list
                    entry.precursors.insert(hdr.sender.name.clone());
                    
                    //Use the rest of the values from the route-entry
                    let flags : RREPFlags = Default::default();
                    let response = RouteResponseMessage {
                        flags: flags,
                        prefix_size: 0, 
                        hop_count: entry.hop_count,
                        destination: msg.destination.clone(),
                        //Uses its own value for the dest_seq_no
                        dest_seq_no: entry.dest_seq_no,
                        originator: msg.originator.clone(),
                        //Lifetime = The stored routes expiration minus the current time, in millis.
                        lifetime: (entry.lifetime.timestamp_millis() - 
                                   Utc::now().timestamp_millis()) as u32,
                    };
                    let mut resp_hdr = MessageHeader::new();
                    let mut dest = Peer::new();
                    dest.name = hdr.sender.name.clone();
                    resp_hdr.destination = dest;
                    resp_hdr.sender = me.clone();
                    resp_hdr.payload = Some(serialize_message(Messages::RREP(response))?);

                    //Before responding
                    //Save the next-hop to the RREQ destination
                    let next_hop = entry.next_hop.clone();
                    //Get the RT entry for the RREQ originator
                    if let Some(entry) = rt.get_mut(&msg.originator) {
                        //and update reverse-route entry's precursor list
                        entry.precursors.insert(next_hop);
                    }
                    
                    // and finally, reply with the RREP
                    return Ok(Some(resp_hdr))
            }
            // Stored route is not valid. Check however, if the dest_seq_no is larger
            // than the one in the message and use it for forwarding if so.
            msg.dest_seq_no = std::cmp::max(msg.dest_seq_no, entry.dest_seq_no);
        }
        
        //NOT producing an RREP. Prepare to forward RREQ.
        //Decreate TTL of msg by 1
        hdr.ttl -= 1;
        hdr.payload = Some(serialize_message(Messages::RREQ(msg))?);
        hdr.sender = me.clone();

        //Forward RREQ
        Ok(Some(hdr))
    }

    fn process_route_response_msg(
        mut hdr : MessageHeader,
        mut msg : RouteResponseMessage,
        route_table : Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        seq_no: Arc<AtomicU32>,
        me : Peer,
        rng: Arc<Mutex<StdRng>>,
        logger : &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> { 
        info!(logger, "RREP message received from {}", &hdr.sender.name);

        //RREPs are UNICASTED. Is this the destination in the header? It not, exit.
        if hdr.destination.name != me.name {
            info!(logger, "Not meant for this node"; "STATUS" => "DROPPED");
            return Ok(None)
        }

        //Update hop-count
        msg.hop_count += 1;
        let mut rt = route_table
            .lock()
            .expect("Could not lock route table");
        
        // Should we update route to DESTINATON with the RREP data?
        let mut entry = rt.entry(msg.destination.clone()).or_insert_with(|| { 
            RouteTableEntry::new(&msg.destination, &hdr.sender.name, Some(msg.dest_seq_no))
        });
        // RFC(6.7) - (i) the sequence number in the routing table is marked as invalid in route table entry.
        if  !entry.flags.contains(RTEFlags::VALID_SEQ_NO) ||
        // RFC(6.7) - (ii) the Destination Sequence Number in the RREP is greater than the node’s copy of 
        // the destination sequence number and the known value is valid, or
            entry.dest_seq_no < msg.dest_seq_no ||
        // RFC(6.7) - (iii) the sequence numbers are the same, but the route is is marked as inactive, or
            (entry.dest_seq_no == msg.dest_seq_no &&
            !entry.flags.contains(RTEFlags::ACTIVE_ROUTE)) || 
        // RFC(6.7) - (iv) the sequence numbers are the same, and the New Hop Count is smaller than the 
        // hop count in route table entry.
            (entry.dest_seq_no == msg.dest_seq_no &&
            msg.hop_count < entry.hop_count) {
                // -  the route is marked as active,
                // -  the destination sequence number is marked as valid,
                entry.flags = entry.flags | RTEFlags::ACTIVE_ROUTE | RTEFlags::VALID_SEQ_NO;
                // -  the next hop in the route entry is assigned to be the node from
                //     which the RREP is received, which is indicated by the source IP
                //     address field in the IP header,
                entry.next_hop = hdr.sender.name.clone();
                // -  the hop count is set to the value of the New Hop Count,
                entry.hop_count = msg.hop_count;
                // -  the expiry time is set to the current time plus the value of the
                //     Lifetime in the RREP message,
                entry.lifetime = Utc::now() + Duration::milliseconds(msg.lifetime as i64);
                // -  and the destination sequence number is the Destination Sequence
                //     Number in the RREP message.
                entry.dest_seq_no = msg.dest_seq_no;
        }

        //Is this node the ORIGINATOR?
        if msg.originator == me.name {
            //What to do?
            info!(logger, "RREP reached its destination!");
        }

        //Update route towards ORIGINATOR
        let neighbours : Vec<String> = rt
            .iter()
            .filter(|(_,v)| v.next_hop == me.name)
            .map(|(k,_)| k.clone())
            .collect();
        let mut entry = rt.entry(msg.originator.clone()).or_insert_with(|| {
            // This node does NOT have a route to ORIGINATOR.
            // Choose a neighbour at random (except the one that sent this RREP).
            let i: usize = { 
                let mut rng = rng.lock().expect("Could not lock RNG");
                rng.gen()
            };
            let next_hop = &neighbours[i % neighbours.len()];
            // Create new route to ORIGINATOR, UNKNOWN SEQ_NO, NEXT_HOP is the chosen neighbour.
            RouteTableEntry::new(&msg.originator, next_hop, None)
        }); 
        // RFC(6.7) - At each node the (reverse) route used to forward a RREP has its lifetime changed to be
        // the maximum of (existing-lifetime, (current time + ACTIVE_ROUTE_TIMEOUT).
        entry.lifetime = std::cmp::max( entry.lifetime, 
                                        Utc::now() + Duration::milliseconds(ACTIVE_ROUTE_TIMEOUT as i64));
        let next_hop_originator = entry.next_hop.clone();

        // RFC(6.7) - When any node transmits a RREP, the precursor list for the
        // corresponding destination node is updated by adding to it the next
        // hop node to which the RREP is forwarded.  
        let entry = rt.get_mut(&msg.destination).unwrap(); //Guaranteed to exist
        entry.precursors.insert(next_hop_originator.clone());
        let next_hop_destination = entry.next_hop.clone();
        
        // RFC(6.7) - Finally, the precursor list for the next hop towards the destination 
        // is updated to contain the next hop towards the source.
        let entry = rt.entry(next_hop_destination.clone()).or_insert_with(|| { 
            RouteTableEntry::new(&next_hop_destination, &me.name, None)
        });
        entry.precursors.insert(next_hop_originator.clone());

        // UNICAST to NEXT_HOP in the route to ORIGINATOR
        let mut resp_hdr = MessageHeader::new();
        resp_hdr.sender = me;
        resp_hdr.destination.name = next_hop_originator;
        resp_hdr.payload = Some(serialize_message(Messages::RREP(msg))?);

        Ok(Some(resp_hdr))
    }

    fn start_flow( 
        dest: String,
        self_peer: Peer,
        data: Vec<u8>,
        short_radio: Arc<dyn Radio>,
    ) -> Result<(), MeshSimError> {
        let dest_peer = Peer {
            name: dest.clone(),
            id: String::from(""),
            short_address: None,
            long_address: None,
        };
        let mut hdr = MessageHeader::new();
        hdr.sender = self_peer;
        hdr.destination = dest_peer;
        hdr.hops = 0;
        hdr.payload = Some(serialize_message(Messages::DATA(DataMessage {
            destination: dest,
            payload: data,
        }))?);

        short_radio.broadcast(hdr)
    }

    fn start_route_discovery(
            &self, 
            destination: String,
            dest_seq_no: u32
        ) -> Result<u32, MeshSimError> {
        let route_id = self.rreq_seq_no.fetch_add(1, Ordering::SeqCst) + 1;
        let mut flags = RREQFlags::GRATUITOUS_RREP;
        if dest_seq_no == 0 {
            flags = flags | RREQFlags::UNKNOWN_SEQUENCE_NUMBER;
        }
        let mut msg = RouteRequestMessage{
            flags,
            hop_count: 0,
            rreq_id: route_id,
            destination: destination.clone(),
            dest_seq_no,
            originator: self.worker_name.clone(),
            orig_seq_no: self.sequence_number.fetch_add(1, Ordering::SeqCst) + 1,
        };


        let mut hdr = MessageHeader::new();
        hdr.sender.id = self.worker_id.clone();
        hdr.sender.name = self.worker_name.clone();
        hdr.destination.name = destination;
        let payload = serialize_message(Messages::RREQ(msg))?;
        hdr.payload = Some(payload);

        self.short_radio.broadcast(hdr)?;
        info!(
            self.logger,
            "Route discovery process started for route_id {}", &route_id
        );

        Ok(route_id)
    }

    fn queue_transmission(
        trans_q: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
        destination: String,
        data: Vec<u8>,
    ) -> Result<(), MeshSimError> {
        let mut tq = trans_q
            .lock()
            .expect("Error trying to acquire lock to transmissions queue");
        let e = tq.entry(destination).or_insert_with(|| vec![]);
        e.push(data);
        Ok(())
    }

    fn get_self_peer(&self) -> Peer {
        Peer {
            name: self.worker_name.clone(),
            id: self.worker_id.clone(),
            short_address: Some(self.short_radio.get_address().into()),
            long_address: None,
        }
    }
}

fn deserialize_message(data: Vec<u8>) -> Result<Messages, MeshSimError> {
    from_slice(data.as_slice()).map_err(|e| {
        let err_msg = String::from("Error deserializing data into message");
        MeshSimError {
            kind: MeshSimErrorKind::Serialization(err_msg),
            cause: Some(Box::new(e)),
        }
    })
}

fn serialize_message(msg: Messages) -> Result<Vec<u8>, MeshSimError> {
    to_vec(&msg).map_err(|e| {
        let err_msg = String::from("Error serializing message");
        MeshSimError {
            kind: MeshSimErrorKind::Serialization(err_msg),
            cause: Some(Box::new(e)),
        }
    })
}