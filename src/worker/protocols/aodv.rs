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
use std::sync::atomic::{AtomicU32, AtomicI64, Ordering};
use std::collections::{HashMap, HashSet};
use std::ops::Add;
use std::default::Default;
use rand::{rngs::StdRng, Rng};
use std::thread;

const CONCCURENT_THREADS_PER_FLOW: usize = 1;

// **************************************************
// ************ Configuration parameters ************
// **************************************************
const ACTIVE_ROUTE_TIMEOUT: u64 = 3000; //milliseconds
const ALLOWED_HELLO_LOSS: usize = 2;
const _BLACKLIST_TIMEOUT: u64 = RREQ_RETRIES as u64 * NET_TRAVERSAL_TIME; //milliseconds
const DELETE_PERIOD: u64 = ALLOWED_HELLO_LOSS as u64 * HELLO_INTERVAL;
const HELLO_INTERVAL: u64 = 1000; //milliseconds
const _LOCAL_ADD_TTL: usize = 2;
const NET_DIAMETER: usize = 35;
// const MIN_REPAIR_TTL
const MY_ROUTE_TIMEOUT: u32 = 2 * ACTIVE_ROUTE_TIMEOUT as u32;
const NODE_TRAVERSAL_TIME: u64 = 40; //milliseconds
const NET_TRAVERSAL_TIME: u64 = 2 * NODE_TRAVERSAL_TIME * NET_DIAMETER as u64; //milliseconds
const NEXT_HOP_WAIT: u64 = NODE_TRAVERSAL_TIME + 10; //milliseconds
const PATH_DISCOVERY_TIME: u64 = 2 * NET_TRAVERSAL_TIME; //milliseconds
const _RERR_RATELIMITE: usize = 10;
const RREQ_RETRIES: usize = 2;
const _RREQ_RATELIMIT: usize = 10;
const TIMEOUT_BUFFER: u64 = 2; //in ??
const _TTL_START: usize = 1;
const _TTL_INCREMENT: usize = 2;
const _TTL_THRESHOLD: usize = 7;
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

#[derive(Debug)]
struct DataCacheEntry {
    destination: String,
    seq_no: u32,
    ts: DateTime<Utc>,
    confirmed: bool,
}

/// Implementation of the Ad-hoc On-Demand Distance Vector routing protocol
#[derive(Debug)]
pub struct AODV {
    worker_name: String,
    worker_id: String,
    short_radio: Arc<dyn Radio>,
    sequence_number: Arc<AtomicU32>,
    rreq_seq_no: Arc<AtomicU32>,
    last_broadcast: Arc<AtomicI64>,
    route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
    pending_routes: Arc<Mutex<HashMap<String, u32>>>,
    queued_transmissions: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
    rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
    data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
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
    destinations: HashMap<String, u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    HELLO(RouteResponseMessage),
}

// **************************************************
// **************** End Messages ********************
// **************************************************

impl Protocol for AODV {
    fn handle_message(
        &self,
        mut hdr: MessageHeader,
        _r_type: RadioTypes,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
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
        let msg_hash = md5::compute(&data);

        let msg = deserialize_message(data)?;
        let route_table = Arc::clone(&self.route_table);
        let seq_no = Arc::clone(&self.sequence_number);
        let last_broadcast = Arc::clone(&self.last_broadcast);
        let rreq_cache = Arc::clone(&self.rreq_cache);
        let data_cache = Arc::clone(&self.data_cache);
        let rng = Arc::clone(&self.rng);
        let pd = Arc::clone(&self.pending_routes);
        let qt = Arc::clone(&self.queued_transmissions);
        let sr = Arc::clone(&self.short_radio);
        AODV::handle_message_internal(
            hdr,
            msg,
            self.get_self_peer(),
            msg_hash,
            route_table,
            rreq_cache,
            data_cache,
            pd,
            qt,
            seq_no,
            last_broadcast,
            sr,
            rng,
            &self.logger,
        )
    }

    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError> {
        let logger = self.logger.clone();
        let _radio = Arc::clone(&self.short_radio);
        let route_table = Arc::clone(&self.route_table);
        let dc = Arc::clone(&self.data_cache);
        let seq_no = Arc::clone(&self.sequence_number);
        let rreq_no = Arc::clone(&self.rreq_seq_no);
        let last_broadcast = Arc::clone(&self.last_broadcast);
        let rreq_cache = Arc::clone(&self.rreq_cache);
        let rng = Arc::clone(&self.rng);
        let pr = Arc::clone(&self.pending_routes);
        let _qt = Arc::clone(&self.queued_transmissions);
        let me = self.get_self_peer();
        let sr = Arc::clone(&self.short_radio);
        let _handle = thread::spawn(move || {
            info!(logger, "Maintenance thread started");
            let _ = AODV::maintenance_loop(
                route_table, 
                rreq_cache, 
                pr,
                dc,
                seq_no,
                rreq_no,
                last_broadcast,
                sr,
                me,
                rng, 
                logger
            );
        });

        info!(self.logger, "Protocol initialized");
        Ok(None)
    }

    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError> {
        let routes = Arc::clone(&self.route_table);
        let pending_routes = Arc::clone(&self.pending_routes);
        let mut dest_seq_no = Default::default();
        let mut valid_route = false;

        //Check if an available route exists for the destination.
        {
            let routes = routes
                .lock()
                .expect("Failed to lock route_table");
            if let Some(route_entry) = routes.get(&destination) {
                //Is the route valid?
                if  route_entry.flags.contains(RTEFlags::VALID_SEQ_NO) &&
                    route_entry.flags.contains(RTEFlags::ACTIVE_ROUTE) {
                    valid_route = true;
                }
                //Invalid route. Get the required data out of the entry and
                //fall through to route discovery.
                dest_seq_no = route_entry.dest_seq_no; //TODO: WHy am I not using this?
            }            
        }

        if valid_route {
            //If one exists, start a new flow with the route
            AODV::start_flow(
                destination,
                Arc::clone(&self.route_table),
                Arc::clone(&self.data_cache),
                self.get_self_peer(),
                data,
                Arc::clone(&self.short_radio),
                self.logger.clone()
            )?;
            return Ok(())
        }
        
        let pending_routes = pending_routes
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
                AODV::start_route_discovery(
                    destination.clone(), 
                    Arc::clone(&self.rreq_seq_no), 
                    Arc::clone(&self.sequence_number), 
                    Arc::clone(&self.last_broadcast), 
                    self.get_self_peer(), 
                    0, 
                    Arc::clone(&self.short_radio), 
                    &self.logger,
                )?
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
        let ts = Utc::now().timestamp_millis();
        AODV {
            worker_name,
            worker_id,
            short_radio,
            sequence_number: Arc::new(AtomicU32::new(0)),
            last_broadcast: Arc::new(AtomicI64::new(ts)),
            rreq_seq_no: Arc::new(AtomicU32::new(starting_rreq_id)),
            route_table: Arc::new(Mutex::new(HashMap::new())),
            pending_routes: Arc::new(Mutex::new(HashMap::new())),
            queued_transmissions: Arc::new(Mutex::new(HashMap::new())),
            rreq_cache: Arc::new(Mutex::new(HashMap::new())),
            data_cache: Arc::new(Mutex::new(HashMap::new())),
            rng,
            logger,
        }
    }

    fn handle_message_internal(
        hdr: MessageHeader,
        msg: Messages,
        me: Peer,
        _msg_hash: Digest,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        pending_routes: Arc<Mutex<HashMap<String, u32>>>,
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
        seq_no: Arc<AtomicU32>,
        last_broadcast: Arc<AtomicI64>,
        short_radio: Arc<dyn Radio>,
        rng: Arc<Mutex<StdRng>>,
        logger: &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
        match msg {
            Messages::DATA(msg) => {
                AODV::process_data_msg(
                    hdr,
                    msg,
                    route_table,
                    data_cache,
                    last_broadcast,
                    me,
                    logger,
                )
            },
            Messages::RREQ(msg) => {
                AODV::process_route_request_msg(
                    hdr,
                    msg,
                    route_table,
                    rreq_cache,
                    seq_no,
                    last_broadcast,
                    me,
                    logger,
                )
            },
            Messages::RREP(msg) => {
                AODV::process_route_response_msg(
                    hdr,
                    msg,
                    route_table,
                    pending_routes,
                    queued_transmissions,
                    data_cache,
                    me,
                    short_radio,
                    rng,
                    logger,
                )
            },
            Messages::REER(msg) => {
                AODV::process_route_err_msg(
                    hdr,
                    msg,
                    route_table,
                    // pending_routes,
                    // queued_transmissions,
                    me,
                    rng,
                    logger,
                )
                // info!(logger, "RERR message received");
                // unimplemented!();
            },
            Messages::RREP_ACK => {
                unimplemented!()
            },
            Messages::HELLO(msg) => {
                AODV::process_hello_msg(hdr, msg, route_table, me, logger)
            }
        }
    }

    fn process_route_request_msg(
        mut hdr : MessageHeader,
        mut msg : RouteRequestMessage,
        route_table : Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        seq_no: Arc<AtomicU32>,
        last_broadcast: Arc<AtomicI64>,
        me : Peer,
        logger : &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
        //Create or update route to hdr.sender
        let mut rt = route_table
            .lock()
            .expect("Error trying to acquire lock on route table");
        let mut entry = rt.entry(hdr.sender.name.clone())
                            // The next hop to an immediate neighbour is just the neighbour
                            .or_insert_with(|| RouteTableEntry::new(&hdr.sender.name, &hdr.sender.name, None));
        entry.lifetime = Utc::now().add(Duration::milliseconds(ACTIVE_ROUTE_TIMEOUT as i64));
        // debug!(logger, "Route table entry: {:#?}", entry);

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
                info!(
                    logger, 
                    "Message received";
                    "msg_type" => "RREQ",
                    "status"=>"DROPPING",
                    "reason"=>"Duplicate",
                    "RREQ_ID" => msg.rreq_id,
                    "Orig" => &msg.originator,
                    "Dest" => &msg.destination,
                    "Hdr.sender" => &hdr.sender.name,
                );
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

        // debug!(logger, "Route table entry: {:#?}", entry);

        //This node generates an RREP if it is itself the destination
        if msg.destination == me.name {
            //RREQ succeeded.
            info!(
                logger, 
                "Message received";
                "msg_type" => "RREQ",
                "status"=>"ACCEPTED",
                "reason"=>"Reached destination",
                "RREQ_ID" => msg.rreq_id,
                "Orig" => &msg.originator,
                "Dest" => &msg.destination,
                "Hdr.sender" => &hdr.sender.name,
            );
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

        //or if it has an active route to the destination... 
        if  let Some(entry) = rt.get_mut(&msg.destination) {
            // AND the dest_seq_no in the RT is greater than or equal to the one in the RREQ
            // AND also valid
            // AND the destination_only flag is not set
            // AND the route is active
            if  entry.dest_seq_no >= msg.dest_seq_no &&
                entry.flags.contains(RTEFlags::VALID_SEQ_NO) &&
                !msg.flags.contains(RREQFlags::DESTINATION_ONLY) &&
                // entry.flags.contains(RTEFlags::ACTIVE_ROUTE) {
                entry.lifetime >= Utc::now() {
                    info!(
                        logger, 
                        "Message received";
                        "msg_type" => "RREQ",
                        "status"=>"ACCEPTED",
                        "reason"=>"A valid route to destination has been found!",
                        "RREQ_ID" => msg.rreq_id,
                        "Orig" => &msg.originator,
                        "Dest" => &msg.destination,
                        "Hdr.sender" => &hdr.sender.name,
                    );
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
        
        info!(
            logger, 
            "Message received";
            "msg_type" => "RREQ",
            "status"=>"FORWARDING",
            "RREQ_ID" => msg.rreq_id,
            "Orig" => &msg.originator,
            "Dest" => &msg.destination,
            "Hdr.sender" => &hdr.sender.name,
        );

        //NOT producing an RREP. Prepare to forward RREQ.
        //Decreate TTL of msg by 1
        hdr.ttl -= 1;
        hdr.payload = Some(serialize_message(Messages::RREQ(msg))?);
        hdr.sender = me.clone();

        //Update the last_broadcast timer
        last_broadcast.store(Utc::now().timestamp_millis(), Ordering::SeqCst);

        //Forward RREQ
        Ok(Some(hdr))
    }

    fn process_route_response_msg(
        hdr : MessageHeader,
        mut msg : RouteResponseMessage,
        route_table : Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        pending_routes: Arc<Mutex<HashMap<String, u32>>>,
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        me : Peer,
        short_radio: Arc<dyn Radio>,
        rng: Arc<Mutex<StdRng>>,
        logger : &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
        //RREPs are UNICASTED. Is this the destination in the header? It not, exit.
        if hdr.destination.name != me.name {
            info!(
                logger, 
                "Message received";
                "msg_type" => "RREP",
                "status"=>"DROPPING",
                "reason"=>"Not meant for this node",
                "Orig" => &msg.originator,
                "Dest" => &msg.destination,
                "Hdr.sender" => &hdr.sender.name,
                "Hdr.dest" => &hdr.destination.name,
            );
            return Ok(None)
        }

        //Update hop-count
        msg.hop_count += 1;
        
        {
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
                    info!(logger, "Updating route to {}", &msg.destination);
                    // -  the route is marked as active,
                    // -  the destination sequence number is marked as valid,
                    entry.flags.insert(RTEFlags::ACTIVE_ROUTE | RTEFlags::VALID_SEQ_NO);
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
        }
         
        
        //Is this node the ORIGINATOR?
        if msg.originator == me.name {
            info!(
                logger, 
                "Message received";
                "msg_type" => "RREP",
                "status"=>"ACCEPTED",
                "Orig" => &msg.originator,
                "Dest" => &msg.destination,
                "Hdr.sender" => &hdr.sender.name,
                "Hdr.dest" => &hdr.destination.name,
            );
            // Remove this node from the pending destinations
            {
                let mut pd = pending_routes
                    .lock()
                    .expect("Error trying to acquire lock to pending_destinations table");
                let _res = pd.remove(&hdr.sender.name);
            }

            //Start any flows that were waiting on the route
            let _ = AODV::start_queued_flows(
                queued_transmissions,
                Arc::clone(&route_table),
                data_cache,
                msg.destination.clone(),
                me,
                short_radio,
                logger,
            );

            return Ok(None)
        }

        //Update route towards ORIGINATOR
        let mut rt = route_table
                .lock()
                .expect("Could not lock route table");
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
        // Mark route to originator as active
        entry.flags.insert(RTEFlags::ACTIVE_ROUTE | RTEFlags::VALID_SEQ_NO);
        let next_hop_originator = if &entry.next_hop == &me.name {
            msg.originator.clone()
        } else {
            entry.next_hop.clone()
        };

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

        info!(
            logger, 
            "Message received";
            "msg_type" => "RREP",
            "status"=>"FORWARDING",
            "Orig" => &msg.originator,
            "Dest" => &msg.destination,
            "Hdr.sender" => &hdr.sender.name,
            "Hdr.dest" => &hdr.destination.name,
        );

        // UNICAST to NEXT_HOP in the route to ORIGINATOR
        let mut resp_hdr = MessageHeader::new();
        resp_hdr.sender = me;
        resp_hdr.destination.name = next_hop_originator;
        resp_hdr.payload = Some(serialize_message(Messages::RREP(msg))?);

        Ok(Some(resp_hdr))
    }

    fn process_route_err_msg(
        hdr : MessageHeader,
        mut msg : RouteErrorMessage,
        route_table : Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        // pending_routes: Arc<Mutex<HashMap<String, u32>>>,
        // queued_transmissions: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
        me : Peer,
        // short_radio: Arc<dyn Radio>,
        _rng: Arc<Mutex<StdRng>>,
        logger : &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
        let mut affected_neighbours = Vec::new();
        let mut response: Option<MessageHeader> = Default::default();
        let mut num_routes_affected: usize = Default::default();

        //Invalidate existing routes
        let mut rt = route_table.lock().expect("Could not lock route table");
        for (destination, seq_no) in msg.destinations.iter() {
            if let Some(entry) = rt.get_mut(destination) {
                if entry.dest_seq_no < *seq_no {
                    entry.flags.remove(RTEFlags::VALID_ROUTE | RTEFlags::ACTIVE_ROUTE);
                    entry.lifetime = Utc::now() + Duration::milliseconds(DELETE_PERIOD as i64);
                    let mut precursors: Vec<_> = entry.precursors
                        .iter()
                        .map(|v| v.clone())
                        .collect();
                    affected_neighbours.append(&mut precursors);
                    num_routes_affected += 1;
                    debug!(logger, "Invalidating route to {}", destination);
                }
                debug!(logger, "Current route for {} is fresher", &destination);
            }
        }
        //List affected destinations. E.g. Those that rely on any of the destinations in the
        //RouteError message as their next hop.
        let mut affected_destinations = Vec::new();
        for (dest, entry) in rt.iter_mut() {
            if msg.destinations.contains_key(&entry.next_hop) {
                affected_destinations.push(dest.clone());
                msg.destinations.insert(dest.clone(), entry.dest_seq_no);
            }
        }

        //Get all the precursors for the newly affected destinations
        for dest in affected_destinations.iter() {
            if let Some(entry) = rt.get_mut(dest) {
                debug!(logger, "Dest:{} - Precursors: {:?}", dest, &entry.precursors);
                let mut precursors: Vec<_> = entry.precursors
                    .iter()
                    .map(|v| v.clone())
                    .collect();
                affected_neighbours.append(&mut precursors);
            }
        }

        //Deliver an appropriate RERR to such neighbours
        let mut packet_status = "DROPPED";
        if affected_neighbours.len() > 0 {
            let mut resp = MessageHeader::new();
            resp.payload = Some(serialize_message(Messages::REER(msg))?);
            resp.sender = me.clone();
            response = Some(resp);
            packet_status = "FORWARDED";
        }
        info!(
            logger, 
            "Message received";
            "msg_type" => "RERR",
            "sender"=>&hdr.sender.name,
            // "unreachable_dests"=>&msg.destinations,
            "status"=>packet_status,
            "invalidated_routes"=>num_routes_affected,
            "affected_neighbours"=>affected_neighbours.len(),
        );
        Ok(response)
    }

    fn process_data_msg(
        mut hdr : MessageHeader,
        msg : DataMessage,
        route_table : Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        last_broadcast: Arc<AtomicI64>,
        me : Peer,
        logger : &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> {         
        //Mark the packet in the cache as confirmed
        {
            let mut dc = data_cache.lock().expect("Could not lock data cache");
            let payload = serialize_message(Messages::DATA(msg.clone()))?;
            let msg_hash = format!("{:x}", md5::compute(&payload));
            if let Some(entry) = dc.get_mut(&msg_hash) {
                entry.confirmed = true;
            }
        }
        if hdr.destination.name != me.name {
            info!(
                logger,
                "Message received";
                "msg_type" => &msg.destination,
                "msg_type" => "DATA",
                "hdr.sender" => &hdr.sender.name,
                "hdr.dest" => &hdr.destination.name,
                "status"=>"DROPPING",
                "reason"=>"Not meant for this node",
            );
            return Ok(None)
        }

        //Is this the destination of the data?
        if msg.destination == me.name {
            info!(
                logger,
                "Message received";
                "msg_type"=>"DATA",
                "sender"=>&hdr.sender.name,
                "status"=>"ACCEPTED",
                "route_length" => hdr.hops
            );
            return Ok(None)
        }

        //Not the destination. Forwarding message.
        let rt = route_table.lock().expect("Coult not lock route table");
        let (next_hop, dest_seq_no, confirmed) = match rt.get(&msg.destination) {
            Some(entry) => { 
                if  entry.flags.contains(RTEFlags::VALID_SEQ_NO) &&
                    entry.flags.contains(RTEFlags::ACTIVE_ROUTE) {
                    //We have an active and valid route to the destination
                    let confirmed = if entry.next_hop == msg.destination {
                        true
                    } else {
                        false
                    };
                    (entry.next_hop.clone(), entry.dest_seq_no, confirmed)
                } else {
                    // let err_msg = format!("This node does not have a valid route to {}", &msg.destination); 
                    // let err = MeshSimError{
                    //     kind : MeshSimErrorKind::Worker(err_msg),
                    //     cause : None,
                    // };
                    info!(
                        logger,
                        "Message received";
                        "msg_type"=>"DATA",
                        "sender"=>&hdr.sender.name,
                        "status"=>"Generating RouteError",
                        "reason"=>"Route to destination is marked as invalid",
                    );

                    //No valid route, so reply with an RERR
                    let mut dest = HashMap::new();
                    dest.insert(msg.destination.clone(), entry.dest_seq_no);
                    let rerr_msg = RouteErrorMessage{
                        flags: Default::default(),
                        destinations: dest,
                    };
                    let mut resp = MessageHeader::new();
                    resp.payload = Some(serialize_message(Messages::REER(rerr_msg))?);
                    resp.sender = me.clone();
                    return Ok(Some(resp))
                }
            },
            None => {
                let err_msg = format!("This node does not have a route to {}", &msg.destination); 
                let err = MeshSimError{
                    kind : MeshSimErrorKind::Worker(err_msg),
                    cause : None,
                };
                info!(
                    logger,
                    "Message received";
                    "msg_type"=>"DATA",
                    "sender"=>&hdr.sender.name,
                    "status"=>"DROPPING",
                    "reason"=>"No route to destination",
                );
                return Err(err)
            },
        };

        info!(
            logger,
            "Message received";
            "msg_type"=>"DATA",
            "sender"=>&hdr.sender.name,
            "status"=>"FORWARDING",
            "next_hop"=>next_hop.clone(),
        );

        //Update header
        hdr.sender = me.clone();
        hdr.hops += 1;
        hdr.destination.name = next_hop.clone();
        let payload = serialize_message(Messages::DATA(msg))?;
        hdr.payload = Some(payload.clone());

        //Record the packet in the data cache
        let mut dc = data_cache.lock().expect("Could not lock data cache");
        let msg_hash = format!("{:x}", md5::compute(&payload));
        dc.insert(msg_hash, DataCacheEntry{ 
            destination: next_hop.clone(),
            seq_no: dest_seq_no,
            ts: Utc::now() + Duration::milliseconds(NEXT_HOP_WAIT as i64),
            confirmed,
        });

        Ok(Some(hdr))
    }

    fn process_hello_msg(
        hdr: MessageHeader,
        msg: RouteResponseMessage,
        route_table : Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        me : Peer,
        logger : &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> { 
        info!(
            logger,
            "Message received";
            "msg_type" => "HELLO",
            "hdr.sender" => &hdr.sender.name,
        );        
        //Whenever a node receives a Hello message from a neighbor, the node
        //SHOULD make sure that it has an active route to the neighbor, and
        //create one if necessary.  If a route already exists, then the
        //Lifetime for the route should be increased, if necessary, to be at
        //least ALLOWED_HELLO_LOSS * HELLO_INTERVAL.  The route to the
        //neighbor, if it exists, MUST subsequently contain the latest
        //Destination Sequence Number from the Hello message.  The current node
        //can now begin using this route to forward data packets.  Routes that
        //are created by hello messages and not used by any other active routes
        //will have empty precursor lists and would not trigger a RERR message
        //if the neighbor moves away and a neighbor timeout occurs.
        let mut rt = route_table
            .lock()
            .expect("Error trying to acquire lock on route table");
        let mut entry = rt.entry(hdr.sender.name.clone())
                            .or_insert_with(|| RouteTableEntry::new(&hdr.sender.name, &me.name, Some(msg.dest_seq_no)));
        let lifetime = Utc::now() + Duration::milliseconds(ALLOWED_HELLO_LOSS as i64 * HELLO_INTERVAL as i64);
        entry.flags.insert(RTEFlags::ACTIVE_ROUTE);
        entry.lifetime = std::cmp::max(entry.lifetime, lifetime);

        Ok(None)
    }

    fn start_flow( 
        dest: String,
        route_table : Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        self_peer: Peer,
        data: Vec<u8>,
        short_radio: Arc<dyn Radio>,
        logger : Logger,
    ) -> Result<(), MeshSimError> {
        
        let (next_hop, seq_no) = {
            let rt = route_table.lock().expect("Could not lock route table");
            let e = rt.get(&dest).ok_or(MeshSimError {
                kind : MeshSimErrorKind::Worker(format!("No route to {}", &dest)),
                cause : None
            })?;
            debug!(logger, "Obtained route to {}", &dest);

            let p = Peer {
                name: e.next_hop.clone(),
                id: String::from(""),
                short_address: None,
                long_address: None,
            };
            (p, e.dest_seq_no)
        };
        let mut hdr = MessageHeader::new();
        hdr.sender = self_peer;
        hdr.destination = next_hop.clone();
        hdr.hops = 0;
        let payload = serialize_message(Messages::DATA(DataMessage {
            destination: dest,
            payload: data,
        }))?;
        hdr.payload = Some(payload.clone());

        //Record the packet in the data cache
        let mut dc = data_cache.lock().expect("Could not lock data cache");
        let msg_hash = format!("{:x}", md5::compute(&payload));
        dc.insert(msg_hash, DataCacheEntry{ 
            destination: next_hop.name,
            seq_no,
            ts: Utc::now() + Duration::milliseconds(NEXT_HOP_WAIT as i64),
            confirmed: false,
        });

        short_radio.broadcast(hdr)?;
        // info!(logger, "Data has been transmitted");
        Ok(())
    }

    fn start_route_discovery(
            destination: String,
            rreq_seq_no: Arc<AtomicU32>,
            seq_no: Arc<AtomicU32>,
            last_broadcast: Arc<AtomicI64>,
            me: Peer,
            dest_seq_no: u32,
            short_radio: Arc<dyn Radio>,
            logger: &Logger,
        ) -> Result<u32, MeshSimError> {
        let route_id = rreq_seq_no.fetch_add(1, Ordering::SeqCst) + 1;
        let mut flags = RREQFlags::GRATUITOUS_RREP;
        if dest_seq_no == 0 {
            flags = flags | RREQFlags::UNKNOWN_SEQUENCE_NUMBER;
        }
        let msg = RouteRequestMessage{
            flags,
            hop_count: 0,
            rreq_id: route_id,
            destination: destination.clone(),
            dest_seq_no,
            originator: me.name.clone(),
            orig_seq_no: seq_no.fetch_add(1, Ordering::SeqCst) + 1,
        };

        let mut hdr = MessageHeader::new();
        hdr.sender.id = me.id.clone();
        hdr.sender.name = me.name.clone();
        hdr.destination.name = destination;
        let payload = serialize_message(Messages::RREQ(msg))?;
        hdr.payload = Some(payload);

        //Update last broadcast timer
        last_broadcast.store(Utc::now().timestamp_millis(), Ordering::SeqCst);

        short_radio.broadcast(hdr)?;
        info!(
            logger,
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

    fn start_queued_flows(
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
        route_table : Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        destination: String,
        me: Peer,
        short_radio: Arc<dyn Radio>,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        info!(
            logger,
            "Looking for queued flows for {}", &destination
        );

        let entry: Option<(String, Vec<Vec<u8>>)> = {
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
            for data in flows {
                let dest = destination.clone();
                let s = me.clone();
                let radio = Arc::clone(&short_radio);
                let l = logger.clone();
                let rt = Arc::clone(&route_table);
                let dc = Arc::clone(&data_cache);
                thread_pool.execute(move || {
                    match AODV::start_flow(dest, rt, dc, s, data, radio, l.clone()) {
                        Ok(_) => {
                            // All good!
                        }
                        Err(e) => {
                            error!(l, "Failed to start transmission - {}", e);
                        }
                    }
                });
            }
        }

        //Wait for all threads to finish
        thread_pool.join();
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

    fn maintenance_loop(
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        pending_routes: Arc<Mutex<HashMap<String, u32>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        // queued_transmissions: Arc<Mutex<HashMap<String, Vec<Vec<u8>>>>>,
        seq_no: Arc<AtomicU32>,
        rreq_no: Arc<AtomicU32>,
        last_broadcast: Arc<AtomicI64>,
        short_radio: Arc<dyn Radio>,
        me: Peer,
        _rng: Arc<Mutex<StdRng>>,
        logger: Logger,
    ) -> Result<(), MeshSimError> {
        let sleep_time = std::time::Duration::from_millis(HELLO_INTERVAL);

        loop {
            thread::sleep(sleep_time);
            let lb = Arc::clone(&last_broadcast);
            let seq = Arc::clone(&seq_no);
            let mut broken_links = HashMap::new();

            // Route Table operations
            {
                let mut rt = route_table.lock().expect("Could not lock route table");
                //Mark expired routes as inactive
                for (_,v) in rt.iter_mut()
                    .filter(|(_, v)| v.flags.contains(RTEFlags::ACTIVE_ROUTE) && v.lifetime < Utc::now()) {
                    v.flags.remove(RTEFlags::ACTIVE_ROUTE);
                    v.lifetime = Utc::now() + Duration::milliseconds(DELETE_PERIOD as i64);
                }

                //Inactive and expired routes are marked as broken-links.
                for (k, v) in rt.iter_mut()
                    .filter(|(k, v)| !v.flags.contains(RTEFlags::ACTIVE_ROUTE) && 
                                      v.lifetime < Utc::now() &&
                                      &v.next_hop == *k ) {
                        broken_links.insert(k.clone(), v.dest_seq_no);
                }

                //Delete inactive routes whose lifetime (delete time) has passed
                rt.retain(|_, v|    v.flags.contains(RTEFlags::ACTIVE_ROUTE) ||
                                    v.lifetime > Utc::now());

                //Broadcast HELLO message?
                let active_routes: Vec<_> = rt.iter().filter(|(_, v)| v.flags.contains(RTEFlags::ACTIVE_ROUTE)).collect();
                if  active_routes.len() > 0 && 
                    (lb.load(Ordering::SeqCst) + HELLO_INTERVAL as i64) < Utc::now().timestamp_millis() {
                    
                    //Craft HELLO MESSAGE
                    let flags: RREPFlags = Default::default();
                    let msg = RouteResponseMessage{
                        flags,
                        prefix_size: 0, 
                        hop_count: 0,
                        destination: me.name.clone(),
                        dest_seq_no: seq.load(Ordering::SeqCst),
                        originator: String::from("N/A"),
                        lifetime: ALLOWED_HELLO_LOSS as u32 * HELLO_INTERVAL as u32,
                    };
                    //BROADCAST it
                    let mut hdr = MessageHeader::new();
                    hdr.sender = me.clone();
                    hdr.payload = Some(serialize_message(Messages::HELLO(msg))?);
                    match short_radio.broadcast(hdr) {
                        Ok(_) => { 
                            // All good
                            info!(logger,  "Sending HELLO message");
                        },
                        Err(e) => { 
                            error!(
                                logger,
                                "Failed to send HELLO message";
                                "reason"=>format!("{}",e)
                            );
                        },
                    };

                    //UPDATE LAST_BROADCAST time
                    lb.store(Utc::now().timestamp_millis(), Ordering::SeqCst);
                }
                
                //For debugging purposes, print the RouteTable
                debug!(logger, "RouteTable:");
                for e in rt.iter() {
                    debug!(logger, "{:?}", e);
                }
            }
            
            {
                //Routes for unconfirmed, expired DATA msgs are marked as broken.
                let mut dc = data_cache.lock().expect("Could not lock data_cache");
                for (msg, entry) in dc.iter_mut()
                    .filter(|(_, v)| !v.confirmed && v.ts < Utc::now()) {
                    info!(
                        logger,
                        "BROKEN_LINK detected";
                        "reason"=>"Unconfirmed DATA message",
                        "destination"=>&entry.destination,
                    );
                    broken_links.insert(msg.to_string(), entry.seq_no);
                }
            }

            //Broken links to send?
            if broken_links.len() > 0 {
                info!(
                    logger, 
                    "Sending message";
                    "msg_type" => "RERR",
                    "sender"=>&me.name,
                    "status"=>"SENDING",
                    // "unreachable_dests"=>&msg.destinations,
                );
                let rerr_msg = RouteErrorMessage{
                    flags: Default::default(),
                    destinations: broken_links,
                };
                let mut hdr = MessageHeader::new();
                hdr.payload = Some(serialize_message(Messages::REER(rerr_msg))?);
                hdr.sender = me.clone();
                match short_radio.broadcast(hdr) {
                    Ok(_) => { /* All good */ },
                    Err(e) => { 
                        error!(
                            logger,
                            "Failed to send BROKEN_LINK message";
                            "reason"=>format!("{}",e)
                        );
                    },
                };
            }

            //Has any of the pending routes expired?
            let mut rrc = rreq_cache.lock().expect("Could not lock RREQ cache");
            let mut new_routes = Vec::new();

            let mut pd = pending_routes.lock().expect("Could not lock pending routes table");
            for (k, v) in pd.iter() {
                if let Some(cache_entry) = rrc.get(&(me.name.clone(), *v)) {
                    if *cache_entry + Duration::milliseconds(PATH_DISCOVERY_TIME as i64) > Utc::now() {
                        //The pending route has not yet expired
                        continue;
                    }
                }

                //Retry route discovery
                let route_id = AODV::start_route_discovery(
                    k.clone(), 
                    Arc::clone(&rreq_no), 
                    seq.clone(), 
                    lb.clone(), 
                    me.clone(), 
                    0, 
                    Arc::clone(&short_radio), 
                    &logger,
                )?;
                new_routes.push((k.clone(), *v, route_id));
            }                
            
            for (dest, old_route_id, new_route_id) in new_routes {
                //Update pending destinations
                pd.entry(dest.clone())
                  .and_modify(|e| *e = new_route_id)
                  .or_insert(new_route_id);
                //Update route cache
                rrc.remove(&(me.name.clone(), old_route_id));
                rrc.insert((me.name.clone(), new_route_id), Utc::now());
            }            
        }
        #[allow(unreachable_code)]
        Ok(())
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