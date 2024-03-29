//! Implemention of the AODV protocol, per its RFC https://www.rfc-editor.org/info/rfc3561
use crate::worker::protocols::{Transmission, HandleMessageOutcome, Protocol, ProtocolMessages};
use crate::worker::radio::{self, *};
use crate::worker::{MessageHeader, MessageStatus};
use crate::{MeshSimError, MeshSimErrorKind};

use chrono::offset::TimeZone;
use chrono::{DateTime, Duration, Utc};

use rand::{rngs::StdRng, Rng};
use serde_cbor::de::*;
use serde_cbor::ser::*;
use slog::{Logger, Record, Serializer, KV};
use std::collections::{HashMap, HashSet};
use std::default::Default;

use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use crossbeam_channel::Sender;

// **************************************************
// ************ Configuration parameters ************
// **************************************************
const MAINTENANCE_RT_MAINTENANCE: i64 = 1000;
const MAINTENANCE_RD_RETRANSMISSION: i64 = 1000;
const MAINTENANCE_HELLO_THRESHOLD: i64 = 1000;
const ACTIVE_ROUTE_TIMEOUT: i64 = 3000; //milliseconds
const ALLOWED_HELLO_LOSS: usize = 2;
// const _BLACKLIST_TIMEOUT: u64 = RREQ_RETRIES as u64 * NET_TRAVERSAL_TIME; //milliseconds
const DELETE_PERIOD: u64 = ALLOWED_HELLO_LOSS as u64 * HELLO_INTERVAL;
const HELLO_INTERVAL: u64 = 1000; //milliseconds
const LOCAL_ADD_TTL: usize = 2;
const NET_DIAMETER: usize = 35;
// const MIN_REPAIR_TTL
// const MY_ROUTE_TIMEOUT: u32 = 2 * ACTIVE_ROUTE_TIMEOUT as u32; //Lifetime used for RREPs when responding to an RREQ to the current node
const NODE_TRAVERSAL_TIME: i64 = 40; //milliseconds
// const NET_TRAVERSAL_TIME: u64 = 2 * NODE_TRAVERSAL_TIME * NET_DIAMETER as u64; //milliseconds
// const NEXT_HOP_WAIT: u64 = NODE_TRAVERSAL_TIME + 10; //milliseconds
// const PATH_DISCOVERY_TIME: u64 = 2 * NET_TRAVERSAL_TIME; //milliseconds
const _RERR_RATELIMITE: usize = 10;
const RREQ_RETRIES: usize = 2;
const _RREQ_RATELIMIT: usize = 10;
// const TIMEOUT_BUFFER: u64 = 2; //in ??
const _TTL_START: usize = 1;
const _TTL_INCREMENT: usize = 2;
const _TTL_THRESHOLD: usize = 7;
//I'm not actually sure this won't explode, given the precision of the numbers.
//Better to log the value and monitor it on tests.
// lazy_static! {
//     static ref MAX_REPAIR_TTL: usize = {
//         let net = NET_DIAMETER as u16;
//         let net = f64::from(net);
//         let val = (0.3f64 * net).round();
//         let val = val as u32;
//         val as usize
//     };
// }
// const fn get_ring_traversal_time(ttl: u64) -> u64 {
//     // RING_TRAVERSAL_TIME: u64 = 2 * NODE_TRAVERSAL_TIME * (TTL_VALUE + TIMEOUT_BUFFER);
//     let time: u64 = 2 * NODE_TRAVERSAL_TIME * (ttl + TIMEOUT_BUFFER);
//     time
// }

// **************************************************
// ***************** Main struct ********************
// **************************************************
bitflags! {
    #[derive(Default)]
    struct RTEFlags : u32 {
        const VALID_SEQ_NO = 0b00000001;
        // const VALID_ROUTE = 0b00000010;
        const ACTIVE_ROUTE = 0b00000010;
    }
}

#[derive(Debug, Clone)]
struct RouteTableEntry {
    destination: String,
    dest_seq_no: u32,
    flags: RTEFlags,
    nic: String,
    next_hop: String,
    hop_count: u8,
    precursors: HashSet<String>,
    lifetime: DateTime<Utc>,
    repairing: bool,
}

impl RouteTableEntry {
    fn new(dest: &String, next_hop: &String, dest_seq_no: Option<u32>) -> Self {
        let mut flags = RTEFlags::VALID_SEQ_NO;
        let seq = match dest_seq_no {
            Some(seq) => seq,
            None => {
                flags -= RTEFlags::VALID_SEQ_NO;
                0
            }
        };
        RouteTableEntry {
            destination: dest.to_owned(),
            dest_seq_no: seq,
            flags,
            nic: String::from("DEFAULT"),
            next_hop: next_hop.to_owned(),
            hop_count: 0,
            precursors: HashSet::new(),
            lifetime: Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 0, 0),
            repairing: false,
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

#[derive(Debug, Clone, Copy)]
struct PendingRouteEntry {
    rreq_id: u32,
    dest_seq_no: u32,
    retries: usize,
    route_repair: bool,
    lifetime: DateTime<Utc>,
}
#[derive(Debug, Clone, Copy)]
struct Config {
    active_route_timeout: i64,
    net_diameter: usize,
    node_traversal_time: i64,
    allowed_hello_loss: usize,
    rreq_retries: usize,
    max_repair_ttl: usize,
    my_route_timeout: i64,
    path_discovery_time: i64,
    next_hop_wait: i64,
    net_traversal_time: i64,
}

/// Implementation of the Ad-hoc On-Demand Distance Vector routing protocol
#[derive(Debug)]
pub struct AODV {
    worker_name: String,
    worker_id: String,
    config: Config,
    tx_queue: Sender<Transmission>,
    sequence_number: Arc<AtomicU32>,
    rreq_seq_no: Arc<AtomicU32>,
    route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
    pending_routes: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
    queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
    /// Used exclusively to control when RREQ messages are processed or marked as duplicates.
    rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
    data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
    /// Timestamp for the next scheduled route_table maintenance operation
    ts_rt_maintenance: AtomicI64,
    /// Timestamp for the next scheduled route-discovery retransmission operation
    ts_rd_retransmission: AtomicI64,
    /// Timestamp for the next scheduled HELLO message transmission
    ts_last_hello_msg: AtomicI64,
    rng: Arc<Mutex<StdRng>>,
    last_transmission: Arc<AtomicI64>,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RouteRequestMessage {
    flags: RREQFlags,
    pub hop_count: u8,
    pub rreq_id: u32,
    pub destination: String,
    pub dest_seq_no: u32,
    pub originator: String,
    orig_seq_no: u32,
}

bitflags! {
    #[derive(Serialize, Deserialize, Default)]
    struct RREPFlags: u8 {
        const REPAIR = 0b00000001;
        const ACK_REQUIRED = 0b00000010;
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RouteResponseMessage {
    flags: RREPFlags,
    //This field is not used in this implementation. Kept for completeness
    //in accordance to the RFC.
    prefix_size: u8,
    pub hop_count: u8,
    pub destination: String,
    pub dest_seq_no: u32,
    pub originator: String,
    lifetime: u32,
}

bitflags! {
    #[derive(Serialize, Deserialize, Default)]
    struct RERRFlags: u8 {
        const NO_DELETE = 0b00000001;
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RouteErrorMessage {
    flags: RREPFlags,
    pub destinations: HashMap<String, u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataMessage {
    pub destination: String,
    payload: Vec<u8>,
}

///Messages used by the AODV protocol
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Messages {
    RREQ(RouteRequestMessage),
    RREP(RouteResponseMessage),
    RERR(RouteErrorMessage),
    #[allow(non_camel_case_types)]
    RREP_ACK,
    DATA(DataMessage),
    HELLO(RouteResponseMessage),
}

impl KV for Messages {
    fn serialize(&self, _rec: &Record, serializer: &mut dyn Serializer) -> slog::Result {
        match *self {
            Messages::DATA(ref m) => {
                let _ = serializer.emit_str("msg_type", "DATA")?;
                serializer.emit_str("msg_destination", &m.destination)
            }
            Messages::RREQ(ref m) => {
                let _ = serializer.emit_str("msg_type", "RREQ")?;
                let _ = serializer.emit_str("msg_source", &m.originator)?;
                let _ = serializer.emit_str("msg_destination", &m.destination)?;
                serializer.emit_u32("rreq_id", m.rreq_id)
            }
            Messages::RREP(ref m) => {
                let _ = serializer.emit_str("msg_type", "RREP")?;
                let _ = serializer.emit_str("msg.originator", &m.originator)?;
                let _ = serializer.emit_str("msg.destination", &m.destination)?;
                serializer.emit_u32("dest_seq_no", m.dest_seq_no)
            }
            Messages::RERR(ref m) => {
                let _ = serializer.emit_str("msg_type", "RERR")?;
                serializer.emit_usize("msg.num_affected_destinations", m.destinations.len())
            }
            Messages::HELLO(ref _m) => serializer.emit_str("msg_type", "HELLO"),
            Messages::RREP_ACK => serializer.emit_str("msg_type", "RREP_ACK"),
        }
    }
}
// **************************************************
// **************** End Messages ********************
// **************************************************

impl Protocol for AODV {
    fn handle_message(
        &self,
        hdr: MessageHeader,
        ts: DateTime<Utc>,
        r_type: RadioTypes,
    ) -> Result<(), MeshSimError> {
        let _msg_id = hdr.get_msg_id();
        let msg = deserialize_message(&hdr.get_payload())?;
        let route_table = Arc::clone(&self.route_table);
        let seq_no = Arc::clone(&self.sequence_number);
        let rreq_seq_no = Arc::clone(&self.rreq_seq_no);
        let rreq_cache = Arc::clone(&self.rreq_cache);
        let data_cache = Arc::clone(&self.data_cache);
        let _rng = Arc::clone(&self.rng);
        let pd = Arc::clone(&self.pending_routes);
        let qt = Arc::clone(&self.queued_transmissions);

        let resp = AODV::handle_message_internal(
            hdr,
            msg,
            ts,
            self.config,
            self.get_self_peer(),
            route_table,
            rreq_cache,
            data_cache,
            pd,
            qt,
            seq_no,
            rreq_seq_no,
            self.tx_queue.clone(),
            r_type,
            &self.logger,
        )?;
        
        if let Some((resp_hdr, md)) = resp {
            self.tx_queue.send((resp_hdr, md, Utc::now()))
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
        info!(self.logger, "Protocol initialized");
        Ok(None)
    }

    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError> {
        let ts0 = Utc::now();
        let routes = Arc::clone(&self.route_table);
        let pending_routes = Arc::clone(&self.pending_routes);
        let rreq_cache = Arc::clone(&self.rreq_cache);
        let mut dest_seq_no = 0;
        let mut valid_route = false;

        let msg = Messages::DATA(DataMessage {
            destination: destination.clone(),
            payload: data,
        });
        //Create log data
        let log_data = ProtocolMessages::AODV(msg.clone());
        let mut hdr = MessageHeader::new(
            self.get_self_peer(),
            destination.clone(),
            serialize_message(msg)?,
        );
        //Perf_Out_Queued_Start
        hdr.delay = ts0.timestamp_nanos();
        info!(&self.logger, "New message"; "msg_id" => hdr.get_msg_id());

        //Check if an available route exists for the destination.
        {
            let routes = routes.lock().expect("Failed to lock route_table");
            if let Some(route_entry) = routes.get(&destination) {
                dest_seq_no = route_entry.dest_seq_no;
                valid_route = route_entry.flags.contains(RTEFlags::VALID_SEQ_NO)
                    && route_entry.flags.contains(RTEFlags::ACTIVE_ROUTE);
            }
        }

        if valid_route {
            //If one exists, start a new flow with the route
            AODV::start_flow(
                destination,
                self.config,
                Arc::clone(&self.route_table),
                Arc::clone(&self.data_cache),
                hdr,
                log_data,
                self.tx_queue.clone(),
                self.logger.clone(),
            )?;
            return Ok(());
        }

        //Queue the data transmission for when the route is established.
        //We do this before starting the Route Discovery in case we get a very quick response
        //from an immediate neighbour, in which case the the transmission might not yet be queued.
        let qt = Arc::clone(&self.queued_transmissions);
        AODV::queue_transmission(qt, destination.clone(), hdr)?;

        //No valid route exists to the destination.
        //Check if a route discovery to the destination is already underway.
        let route_entry = {
            let pending_routes = pending_routes
                .lock()
                .expect("Failed to lock pending_destinations table");
            pending_routes.get(&destination).copied()
        };
        match route_entry {
            Some(_entry) => {
                info!(
                    self.logger,
                    "Route discovery process already started for {}", &destination
                );
            }
            None => {
                info!(
                    self.logger,
                    "No known route to {}. Starting discovery process.", &destination
                );
                let ttl = None; //Use default value
                let retries = None; //Use default value
                let repair = false;
                AODV::start_route_discovery(
                    destination,
                    dest_seq_no,
                    Arc::clone(&self.rreq_seq_no),
                    self.sequence_number.fetch_add(1, Ordering::SeqCst) + 1,
                    ttl,
                    retries,
                    repair,
                    self.get_self_peer(),
                    self.config,
                    self.tx_queue.clone(),
                    pending_routes,
                    rreq_cache,
                    &self.logger,
                )?
            }
        }

        Ok(())
    }

    fn do_maintenance(&self) -> Result<(), MeshSimError> {
        if Utc::now().timestamp_nanos() >= self.ts_rt_maintenance.load(Ordering::SeqCst) {
            match AODV::route_table_maintenance(
                Arc::clone(&self.route_table),
                Arc::clone(&self.data_cache),
                self.get_self_peer(),
                self.tx_queue.clone(),
                &self.logger,
            ) {
                Ok(_) => {
                    //Update to the new timestamp for doing this maintenance operation
                    let new_ts = Utc::now() + Duration::milliseconds(MAINTENANCE_RT_MAINTENANCE);
                    self.ts_rt_maintenance
                        .store(new_ts.timestamp_nanos(), Ordering::SeqCst);
                }
                Err(e) => {
                    error!(
                        &self.logger,
                        "Failed to perform route table maintenance";
                        "reason"=>format!("{}",e)
                    );
                }
            }
        }

        if Utc::now().timestamp_nanos() >= self.ts_rd_retransmission.load(Ordering::SeqCst) {
            match AODV::route_discovery_maintenance(
                Arc::clone(&self.rreq_cache),
                Arc::clone(&self.pending_routes),
                Arc::clone(&self.route_table),
                Arc::clone(&self.sequence_number),
                Arc::clone(&self.rreq_seq_no),
                self.tx_queue.clone(),
                self.get_self_peer(),
                self.config,
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
                        "Failed to perform route discovery maintenance";
                        "reason"=>format!("{}",e)
                    );
                }
            }
        }

        if Utc::now().timestamp_nanos() >= self.ts_last_hello_msg.load(Ordering::SeqCst) {
            match AODV::hello_msg_maintenance(
                Arc::clone(&self.route_table),
                self.get_self_peer(),
                self.config,
                Arc::clone(&self.sequence_number),
                self.tx_queue.clone(),
                Arc::clone(&self.last_transmission),
                &self.logger,
            ) {
                Ok(_) => {
                    //Update to the new timestamp for doing this maintenance operation
                    let new_ts = Utc::now() + Duration::milliseconds(MAINTENANCE_HELLO_THRESHOLD);
                    self.ts_last_hello_msg
                        .store(new_ts.timestamp_nanos(), Ordering::SeqCst);
                }
                Err(e) => {
                    error!(
                        &self.logger,
                        "Failed to send HELLO message";
                        "reason"=>format!("{}",e)
                    );
                }
            }
        }

        Ok(())
    }
}

impl AODV {
    /// Instantiate a new handler for the AODV protocol
    pub fn new(
        worker_name: String,
        worker_id: String,
        active_route_timeout: Option<i64>,
        net_diameter: Option<usize>,
        node_traversal_time: Option<i64>,
        next_hop_wait: Option<i64>,
        allowed_hello_loss: Option<usize>,
        rreq_retries: Option<usize>,
        tx_queue: Sender<Transmission>,
        rng: Arc<Mutex<StdRng>>,
        last_transmission: Arc<AtomicI64>,
        logger: Logger,
    ) -> Self {
        // let starting_rreq_id: u32 = thread_rng().gen_range(0, std::u32::MAX);
        let starting_rreq_id: u32 = {
            let mut rng = rng.lock().expect("Could not lock RNG");
            rng.gen_range(0..std::u32::MAX)
        };
        let ts_rt_maintenance = Utc::now() + Duration::milliseconds(MAINTENANCE_RT_MAINTENANCE);
        let ts_rt_maintenance = AtomicI64::new(ts_rt_maintenance.timestamp_nanos());
        let ts_rd_retransmission =
            Utc::now() + Duration::milliseconds(MAINTENANCE_RD_RETRANSMISSION);
        let ts_rd_retransmission = AtomicI64::new(ts_rd_retransmission.timestamp_nanos());
        let ts_last_hello_msg = Utc::now() + Duration::milliseconds(MAINTENANCE_HELLO_THRESHOLD);
        let ts_last_hello_msg = AtomicI64::new(ts_last_hello_msg.timestamp_nanos());

        let active_route_timeout = active_route_timeout.unwrap_or(ACTIVE_ROUTE_TIMEOUT);
        let net_diameter = net_diameter.unwrap_or(NET_DIAMETER);
        let node_traversal_time = node_traversal_time.unwrap_or(NODE_TRAVERSAL_TIME);
        let next_hop_wait = next_hop_wait.unwrap_or(node_traversal_time + 10); //milliseconds
        let allowed_hello_loss = allowed_hello_loss.unwrap_or(ALLOWED_HELLO_LOSS);
        let rreq_retries = rreq_retries.unwrap_or(RREQ_RETRIES);
        let net_traversal_time = 2 * node_traversal_time * net_diameter as i64; //milliseconds
        let max_repair_ttl = (0.3f64 * net_diameter as f64).round() as usize;
        //Lifetime used for RREPs when responding to an RREQ to the current node
        let my_route_timeout = 2 * active_route_timeout;
        let path_discovery_time = 2 * net_traversal_time; //milliseconds

        let config = Config {
            active_route_timeout,
            net_diameter,
            node_traversal_time,
            allowed_hello_loss,
            rreq_retries,
            max_repair_ttl,
            my_route_timeout,
            path_discovery_time,
            next_hop_wait,
            net_traversal_time,
        };

        AODV {
            worker_name,
            worker_id,
            config,
            tx_queue,
            sequence_number: Arc::new(AtomicU32::new(0)),
            rreq_seq_no: Arc::new(AtomicU32::new(starting_rreq_id)),
            route_table: Arc::new(Mutex::new(HashMap::new())),
            pending_routes: Arc::new(Mutex::new(HashMap::new())),
            queued_transmissions: Arc::new(Mutex::new(HashMap::new())),
            rreq_cache: Arc::new(Mutex::new(HashMap::new())),
            data_cache: Arc::new(Mutex::new(HashMap::new())),
            ts_rt_maintenance,
            ts_rd_retransmission,
            ts_last_hello_msg,
            rng,
            last_transmission,
            logger,
        }
    }

    fn handle_message_internal(
        hdr: MessageHeader,
        msg: Messages,
        ts: DateTime<Utc>,
        config: Config,
        me: String,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        pending_routes: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        seq_no: Arc<AtomicU32>,
        rreq_seq_no: Arc<AtomicU32>,
        tx_queue: Sender<Transmission>,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        AODV::connectivity_update(
            &hdr,
            ts,
            config,
            Arc::clone(&route_table),
            Arc::clone(&data_cache),
        )?;
        match msg {
            Messages::DATA(msg) => AODV::process_data_msg(
                hdr,
                msg,
                config,
                route_table,
                data_cache,
                pending_routes,
                queued_transmissions,
                rreq_cache,
                seq_no,
                rreq_seq_no,
                config.active_route_timeout,
                me,
                tx_queue,
                r_type,
                logger,
            ),
            Messages::RREQ(msg) => AODV::process_route_request_msg(
                hdr,
                msg,
                config,
                route_table,
                rreq_cache,
                seq_no,
                me,
                r_type,
                logger,
            ),
            Messages::RREP(msg) => AODV::process_route_response_msg(
                hdr,
                msg,
                route_table,
                pending_routes,
                queued_transmissions,
                data_cache,
                config.active_route_timeout,
                me,
                config,
                tx_queue,
                r_type,
                logger,
            ),
            Messages::RERR(msg) => {
                AODV::process_route_err_msg(
                    hdr,
                    msg,
                    route_table,
                    me,
                    r_type,
                    logger,
                )
                // info!(logger, "RERR message received");
                // unimplemented!();
            }
            Messages::RREP_ACK => unimplemented!(),
            Messages::HELLO(msg) => {
                AODV::process_hello_msg(hdr, msg, config, route_table, me, r_type, logger)
            }
        }
    }

    fn process_route_request_msg(
        hdr: MessageHeader,
        mut msg: RouteRequestMessage,
        config: Config,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        seq_no: Arc<AtomicU32>,
        me: String,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        //Check if this is a duplicate RREQ
        {
            let mut rr_cache = rreq_cache
                .lock()
                .expect("Error trying to acquire lock on rreq_cache");
            let cache_entry = rr_cache
                .entry((msg.originator.clone(), msg.rreq_id))
                .or_insert(Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 0, 0));
            let threshold_time = *cache_entry + Duration::milliseconds(config.path_discovery_time);
            debug!(logger, "Threshold time: {:?}", &threshold_time);
            if threshold_time > Utc::now() {
                //Duplicate RREQ. Drop.
                radio::log_handle_message(
                    logger,
                    &hdr,
                    MessageStatus::DROPPED,
                    Some("DUPLICATE"),
                    None,
                    r_type,
                    &Messages::RREQ(msg),
                );
                *cache_entry = Utc::now();
                return Ok(None);
            }
            //Update the cache to the last time we received this RREQ
            *cache_entry = Utc::now();
        }

        //Increase msg hop_count
        msg.hop_count += 1;

        //Create/update route to msg.originator
        let entry = {
            let mut rt = route_table
                .lock()
                .expect("Error trying to acquire lock on route table");
            let mut entry = rt.entry(msg.originator.clone()).or_insert_with(|| {
                RouteTableEntry::new(&msg.originator, &hdr.sender, Some(msg.orig_seq_no))
            });
            entry.dest_seq_no = std::cmp::max(entry.dest_seq_no, msg.orig_seq_no);
            entry.flags.insert(RTEFlags::VALID_SEQ_NO); //Should this be done only if the seq_no is updated?
            entry.hop_count = msg.hop_count;
            let minimal_lifetime = Utc::now() + Duration::milliseconds(2 * config.net_traversal_time)
                - Duration::milliseconds(2 * entry.hop_count as i64 * config.node_traversal_time);
            entry.lifetime = std::cmp::max(entry.lifetime, minimal_lifetime);
            entry.clone()
        };


        // debug!(logger, "Route table entry: {:#?}", entry);

        //This node generates an RREP if it is itself the destination
        if msg.destination == me {
            //RREQ succeeded.
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::ACCEPTED,
                Some("RREQ reached its destination"),
                None,
                r_type,
                &Messages::RREQ(msg.clone()),
            );

            //If the message contains the curent seq_no, update it.
            let _old_seq =
                seq_no.compare_and_swap(msg.dest_seq_no, msg.dest_seq_no + 1, Ordering::SeqCst);
            let flags: RREPFlags = Default::default();
            let response = RouteResponseMessage {
                flags,
                prefix_size: 0,
                hop_count: 0,
                destination: me.clone(),
                dest_seq_no: seq_no.load(Ordering::SeqCst),
                originator: msg.originator,
                lifetime: config.my_route_timeout as u32,
            };

            //Tag the response
            let response = Messages::RREP(response);
            //Create the log data for the packet
            let log_data = ProtocolMessages::AODV(response.clone());

            let resp_hdr =
                MessageHeader::new(me, entry.next_hop, serialize_message(response)?);

            return Ok(Some((resp_hdr, log_data)));
        }

        //or if it has an active route to the destination...
        {
            let mut rt = route_table
                .lock()
                .expect("Error trying to acquire lock on route table");
            if let Some(entry) = rt.get_mut(&msg.destination) {
                // AND the dest_seq_no in the RT is greater than or equal to the one in the RREQ
                // AND also valid
                // AND the destination_only flag is not set
                // AND the route is active
                if entry.dest_seq_no >= msg.dest_seq_no &&
                    entry.flags.contains(RTEFlags::VALID_SEQ_NO) &&
                    !msg.flags.contains(RREQFlags::DESTINATION_ONLY) &&
                    // entry.flags.contains(RTEFlags::ACTIVE_ROUTE) {
                    entry.lifetime >= Utc::now()
                {
                    radio::log_handle_message(
                        logger,
                        &hdr,
                        MessageStatus::ACCEPTED,
                        Some("A valid route to destination has been found!"),
                        Some("RREP process will be initiated"),
                        r_type,
                        &Messages::RREQ(msg.clone()),
                    );

                    //Update forward-route entry's precursor list
                    entry.precursors.insert(hdr.sender.clone());

                    //Use the rest of the values from the route-entry
                    let flags: RREPFlags = Default::default();
                    let response = RouteResponseMessage {
                        flags,
                        prefix_size: 0,
                        hop_count: entry.hop_count,
                        destination: msg.destination.clone(),
                        //Uses its own value for the dest_seq_no
                        dest_seq_no: entry.dest_seq_no,
                        originator: msg.originator.clone(),
                        //Lifetime = The stored routes expiration minus the current time, in millis.
                        lifetime: (entry.lifetime.timestamp_millis() - Utc::now().timestamp_millis())
                            as u32,
                    };

                    //Tag the response
                    let response = Messages::RREP(response);
                    //Create the log data for the packet
                    let log_data = ProtocolMessages::AODV(response.clone());

                    let dest = hdr.sender;
                    // let resp_hdr = hdr
                    //     .create_forward_header(me)
                    //     .set_destination(dest)
                    //     .set_payload(serialize_message(response)?)
                    //     .build();
                    let resp_hdr = MessageHeader::new(
                        me,
                        dest,
                        serialize_message(response)?
                    );
                    //Before responding
                    //Save the next-hop to the RREQ destination
                    let next_hop = entry.next_hop.clone();
                    //Get the RT entry for the RREQ originator
                    if let Some(entry) = rt.get_mut(&msg.originator) {
                        //and update reverse-route entry's precursor list
                        entry.precursors.insert(next_hop);
                    }

                    return Ok(Some((resp_hdr, log_data)));
                }
                // Stored route is not valid. Check however, if the dest_seq_no is larger
                // than the one in the message and use it for forwarding if so.
                msg.dest_seq_no = std::cmp::max(msg.dest_seq_no, entry.dest_seq_no);
            }
        }


        //Re-tag the message
        let msg = Messages::RREQ(msg);

        radio::log_handle_message(
            logger,
            &hdr,
            MessageStatus::FORWARDING,
            Some("No route to destination"),
            None,
            r_type,
            &msg,
        );
        //Create log data
        let log_data = ProtocolMessages::AODV(msg.clone());
        //NOT producing an RREP. Prepare to forward RREQ.
        let fwd_hdr = hdr
            .create_forward_header(me)
            .set_payload(serialize_message(msg)?)
            .build();

        Ok(Some((fwd_hdr, log_data)))
    }

    fn process_route_response_msg(
        hdr: MessageHeader,
        mut msg: RouteResponseMessage,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        pending_routes: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        active_route_timeout: i64,
        me: String,
        config: Config,
        tx_queue: Sender<Transmission>,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        //RREPs are UNICASTED. Is this the destination in the header? It not, exit.
        if hdr.destination != me {
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::DROPPED,
                Some("Not meant for this node"),
                None,
                r_type,
                &Messages::RREP(msg),
            );
            return Ok(None);
        }

        //Update hop-count
        msg.hop_count += 1;

        let _repaired = {
            let mut rt = route_table.lock().expect("Could not lock route table");
            // Should we update route to DESTINATON with the RREP data?
            let mut entry = rt.entry(msg.destination.clone()).or_insert_with(|| {
                RouteTableEntry::new(&msg.destination, &hdr.sender, Some(msg.dest_seq_no))
            });
            // RFC(6.7) - (i) the sequence number in the routing table is marked as invalid in route table entry.
            if !entry.flags.contains(RTEFlags::VALID_SEQ_NO) ||
            // RFC(6.7) - (ii) the Destination Sequence Number in the RREP is greater than the node’s copy of 
            // the destination sequence number and the known value is valid, or
                entry.dest_seq_no < msg.dest_seq_no ||
            // RFC(6.7) - (iii) the sequence numbers are the same, but the route is is marked as inactive, or
                (entry.dest_seq_no == msg.dest_seq_no &&
                !entry.flags.contains(RTEFlags::ACTIVE_ROUTE)) ||
            // RFC(6.7) - (iv) the sequence numbers are the same, and the New Hop Count is smaller than the 
            // hop count in route table entry.
                (entry.dest_seq_no == msg.dest_seq_no &&
                msg.hop_count < entry.hop_count)
            {
                info!(logger, "Updating route to {}", &msg.destination);
                // -  the route is marked as active,
                // -  the destination sequence number is marked as valid,
                entry
                    .flags
                    .insert(RTEFlags::ACTIVE_ROUTE | RTEFlags::VALID_SEQ_NO);
                // -  the next hop in the route entry is assigned to be the node from
                //     which the RREP is received, which is indicated by the source IP
                //     address field in the IP header,
                entry.next_hop = hdr.sender.clone();
                // -  the hop count is set to the value of the New Hop Count,
                entry.hop_count = msg.hop_count;
                // -  the expiry time is set to the current time plus the value of the
                //     Lifetime in the RREP message,
                entry.lifetime = Utc::now() + Duration::milliseconds(msg.lifetime as i64);
                // -  and the destination sequence number is the Destination Sequence
                //     Number in the RREP message.
                entry.dest_seq_no = msg.dest_seq_no;
            }

            //If the route to this destination was in repair, mark it as repaired
            std::mem::replace(&mut entry.repairing, false)
        };

        //Is this node the ORIGINATOR?
        if msg.originator == me {
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::ACCEPTED,
                None,
                None,
                r_type,
                &Messages::RREP(msg.clone()),
            );

            // Remove this node from the pending destinations
            {
                let mut pd = pending_routes
                    .lock()
                    .expect("Error trying to acquire lock to pending_destinations table");
                let _res = pd.remove(&hdr.sender);
                debug!(logger, "Route to {} marked as complete", &msg.destination);
            }

            //TODO: If the RREP was generated as part of a RouteRepair, this is the moment where the RERR with
            //the N flag can be generated by reading the repaired variable.

            //Start any flows that were waiting on the route
            let _ = AODV::start_queued_flows(
                queued_transmissions,
                Arc::clone(&route_table),
                data_cache,
                msg.destination,
                me,
                config,
                tx_queue,
                logger,
            );

            return Ok(None);
        }

        //Update route towards ORIGINATOR
        let next_hop_originator = {
            let mut rt = route_table.lock().expect("Could not lock route table");
            let mut entry = match rt.get_mut(&msg.originator) {
                Some(entry) => entry,
                None => {
                    //A route towards the originator should have been created when the RREQ was processed.
                    //If the route no longer exists, that is likely due to delay created by congestion and/or long routes.
                    //Regardless, this RREP can't proceed. Drop the packet
                    radio::log_handle_message(
                        logger,
                        &hdr,
                        MessageStatus::DROPPED,
                        Some("No route to RREQ originator"),
                        Some("RREP_CANCELLED"),
                        r_type,
                        &Messages::RREP(msg),
                    );
                    return Ok(None);
                }
            };
    
            // RFC(6.7) - At each node the (reverse) route used to forward a RREP has its lifetime changed to be
            // the maximum of (existing-lifetime, (current time + ACTIVE_ROUTE_TIMEOUT).
            entry.lifetime = std::cmp::max(
                entry.lifetime,
                Utc::now() + Duration::milliseconds(active_route_timeout),
            );
            // Mark route to originator as active
            entry
                .flags
                .insert(RTEFlags::ACTIVE_ROUTE | RTEFlags::VALID_SEQ_NO);
            
            if &entry.next_hop == &me {
                msg.originator.clone()
            } else {
                entry.next_hop.clone()
            }
        };

        // RFC(6.7) - When any node transmits a RREP, the precursor list for the
        // corresponding destination node is updated by adding to it the next
        // hop node to which the RREP is forwarded.
        {
            let mut rt = route_table.lock().expect("Could not lock route table");
            let entry = rt.get_mut(&msg.destination).unwrap(); //Guaranteed to exist
            entry.precursors.insert(next_hop_originator.clone());
            let next_hop_destination = entry.next_hop.clone();

            // RFC(6.7) - Finally, the precursor list for the next hop towards the destination
            // is updated to contain the next hop towards the source.
            let entry = rt
                .entry(next_hop_destination.clone())
                .or_insert_with(|| RouteTableEntry::new(&next_hop_destination, &me, None));
            entry.precursors.insert(next_hop_originator.clone());
            entry.lifetime = std::cmp::max(
                entry.lifetime,
                Utc::now() + Duration::milliseconds(active_route_timeout),
            );            
        }

        //Re-tag message
        let msg = Messages::RREP(msg);

        radio::log_handle_message(logger, &hdr, MessageStatus::FORWARDING, None, None, r_type, &msg);

        //Create log data
        let log_data = ProtocolMessages::AODV(msg.clone());

        //Build the forwarding header
        let fwd_hdr = hdr
            .create_forward_header(me)
            .set_destination(next_hop_originator)
            .set_payload(serialize_message(msg)?)
            .build();

        Ok(Some((fwd_hdr, log_data)))
    }

    fn process_route_err_msg(
        hdr: MessageHeader,
        mut msg: RouteErrorMessage,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        me: String,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        let affected_neighbours = {
            let mut affected_neighbours = Vec::new();
            let mut rt = route_table
                .lock()
                .expect("Could not lock route table");
            //Invalidate existing routes
            for (destination, seq_no) in msg.destinations.iter() {
                if let Some(entry) = rt.get_mut(destination) {
                    if entry.dest_seq_no < *seq_no {
                        entry
                            .flags
                            .remove(RTEFlags::VALID_SEQ_NO | RTEFlags::ACTIVE_ROUTE);
                        entry.lifetime = Utc::now() + Duration::milliseconds(DELETE_PERIOD as i64);
                        let mut precursors: Vec<_> = entry.precursors.iter().cloned().collect();
                        affected_neighbours.append(&mut precursors);
                        debug!(logger, "Invalidating route to {}", destination);
                    }
                    debug!(logger, "Current route for {} is fresher", &destination);
                }
            }

            // Iterate through the node's route table. If any of the entries rely on a node listed
            // in the RERR msg as its next hop, we mark it as affected, since it can no longer be reached.
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
                    debug!(
                        logger,
                        "Dest:{} - Precursors: {:?}", dest, &entry.precursors
                    );
                    let mut precursors: Vec<_> = entry.precursors.iter().cloned().collect();
                    affected_neighbours.append(&mut precursors);
                }
            }
            affected_neighbours
        };

        //Deliver an appropriate RERR to such neighbours
        if !affected_neighbours.is_empty() {
            //Re-tag message
            let msg = Messages::RERR(msg);
            //Build log data
            let log_data = ProtocolMessages::AODV(msg.clone());

            //Build the response msg
            let rsp_hdr = hdr
                .create_forward_header(me)
                .set_payload(serialize_message(msg)?)
                .build();

            Ok(Some((rsp_hdr, log_data)))
        } else {
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::DROPPED,
                Some("No routes affected"),
                None,
                r_type,
                &Messages::RERR(msg),
            );

            Ok(None)
        }
    }

    //TODO: Update lifetime of route to next_hop.
    fn process_data_msg(
        hdr: MessageHeader,
        msg: DataMessage,
        config: Config,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        pending_routes: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        seq_no: Arc<AtomicU32>,
        rreq_seq_no: Arc<AtomicU32>,
        active_route_timeout: i64,
        me: String,
        tx_queue: Sender<Transmission>,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        //Mark the packet in the cache as confirmed if applicable
        let _confirmed = {
            let mut dc = data_cache.lock().expect("Could not lock data cache");
            if let Some(entry) = dc.get_mut(hdr.get_msg_id()) {
                entry.confirmed = true;
                true
            } else {
                false
            }
        };

        if hdr.destination != me {
            //Data packets are "unicasted" to the next hop in the route.
            //If this node is not the intended destination in the header, drop it.
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::DROPPED,
                Some("Not meant for this node"),
                None,
                r_type,
                &Messages::DATA(msg),
            );
            return Ok(None);
        }

        //Is this the destination of the data?
        if msg.destination == me {
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::ACCEPTED,
                None,
                None,
                r_type,
                &Messages::DATA(msg),
            );
            return Ok(None);
        }

        //Not the destination. Forwarding message.
        let (next_hop, dest_seq_no, confirmed) = {
            let entry = {
                let rt = route_table.lock().expect("Coult not lock route table");
                let e = rt.get(&msg.destination);
                e.cloned()
            };
            
            match entry {
                Some(entry) => {
                    if !entry.flags.contains(RTEFlags::VALID_SEQ_NO) {
                        //This is considered a route error.
                        //Proceed to process this route error per RFC section 6.11, case (i)
                        // (i) if it detects a link break for the next hop of an active
                        // route in its routing table while transmitting data (and
                        // route repair, if attempted, was unsuccessful)
                        // For case (i), the node first makes a list of unreachable destinations
                        // consisting of the unreachable neighbor and any additional destinations
                        // (or subnets, see section 7) in the local routing table that use the
                        // unreachable neighbor as the next hop.
                        //
                        // For this particular case, we assume having the route marked as invalid counts
                        // as a link break.
                        radio::log_handle_message(
                            logger,
                            &hdr,
                            MessageStatus::DROPPED,
                            Some("Route to destination is marked as invalid"),
                            Some("Generating RouteError"),
                            r_type,
                            &Messages::DATA(msg.clone()),
                        );
    
                        //No valid route, so reply with an RERR
                        let mut dest = HashMap::new();
                        dest.insert(msg.destination, entry.dest_seq_no);
                        let rerr_msg = RouteErrorMessage {
                            flags: Default::default(),
                            destinations: dest,
                        };
    
                        //Tag response
                        let rerr_msg = Messages::RERR(rerr_msg);
                        //Build log data
                        let log_data = ProtocolMessages::AODV(rerr_msg.clone());
    
                        //Build header
                        let hdr = MessageHeader::new(
                            me,
                            hdr.sender,
                            serialize_message(rerr_msg)?,
                        );
    
                        return Ok(Some((hdr, log_data)));
                    }
    
                    if entry.flags.contains(RTEFlags::ACTIVE_ROUTE) {
                        //We have an active and valid route to the destination
                        //Update the lifetime to the destination
                        let mut rt = route_table.lock().expect("Coult not lock route table");
                        let e = rt.entry(msg.destination.clone())
                            .or_insert(entry);
                        e.lifetime = std::cmp::max(
                            e.lifetime,
                            Utc::now() + Duration::milliseconds(active_route_timeout),
                        );
                        let confirmed = e.next_hop == msg.destination;
                        (e.next_hop.clone(), e.dest_seq_no, confirmed)
                    } else {
                        //Route is inactive. Attempt to repair it.
                        //If it's already repairing, just queue the transmission.
                        //Note: Make sure none of the FOUR tables being passed are currently locked, or it will
                        //create a deadlock, as it once did.
                        if entry.repairing
                            || AODV::route_repair(
                                &msg.destination,
                                Arc::clone(&route_table),
                                Arc::clone(&rreq_seq_no),
                                Arc::clone(&seq_no),
                                me.clone(),
                                config,
                                tx_queue,
                                Arc::clone(&pending_routes),
                                Arc::clone(&rreq_cache),
                                &logger,
                            )?
                        {
                            AODV::queue_transmission(
                                queued_transmissions,
                                msg.destination.clone(),
                                hdr.clone(),
                            )?;
                            radio::log_handle_message(
                                logger,
                                &hdr,
                                MessageStatus::QUEUED,
                                Some("Route to destination being repaired"),
                                None,
                                r_type,
                                &Messages::DATA(msg),
                            );
                        } else {
                            // This is considered a route error.
                            // Proceed to process this route error per RFC section 6.11, case (ii).
                            //
                            // (ii) if it gets a data packet destined to a node for which it
                            // does not have an active route and is not repairing (if
                            // using local repair).
                            //
                            // In this case, failing to run route_repair counts as "is not repairing".
                            radio::log_handle_message(
                                logger,
                                &hdr,
                                MessageStatus::DROPPED,
                                Some("RouteHealing was not possible"),
                                Some("Generating RouteError"),
                                r_type,
                                &Messages::DATA(msg.clone()),
                            );
    
                            // For case (ii), there is only one unreachable destination, which is the
                            // destination of the data packet that cannot be delivered.
                            let mut dest = HashMap::new();
                            dest.insert(msg.destination, entry.dest_seq_no);
                            let rerr_msg = RouteErrorMessage {
                                flags: Default::default(),
                                destinations: dest,
                            };
    
                            //Tag response
                            let rerr_msg = Messages::RERR(rerr_msg);
                            //Build log data
                            let log_data = ProtocolMessages::AODV(rerr_msg.clone());
    
                            //Build header
                            let resp_hdr = MessageHeader::new(
                                me,
                                hdr.sender, //Destination should be hdr.sender, but need to check
                                serialize_message(rerr_msg)?,
                            );
    
                            return Ok(Some((resp_hdr, log_data)));
                        }
    
                        return Ok(None);
                    }
                }
                None => {
                    warn!(logger, "This node does not have a route to {}", &msg.destination);
                    // let err_msg = format!("This node does not have a route to {}", &msg.destination);
                    // let err = MeshSimError {
                    //     kind: MeshSimErrorKind::Worker(err_msg),
                    //     cause: None,
                    // };
                    radio::log_handle_message(
                        logger,
                        &hdr,
                        MessageStatus::DROPPED,
                        Some("No route to destination"),
                        None,
                        r_type,
                        &Messages::DATA(msg),
                    );
                    return Ok(None);
                }
            }
        };

        {
            //Record the packet in the data cache
            let mut dc = data_cache
                .lock()
                .expect("Could not lock data cache");
            dc.insert(
                hdr.get_msg_id().to_string(),
                DataCacheEntry {
                    destination: next_hop.clone(),
                    seq_no: dest_seq_no,
                    ts: Utc::now() + Duration::milliseconds(config.next_hop_wait),
                    confirmed,
                },
            );
        }

        //Re-tag the msg
        let msg = Messages::DATA(msg);
        //Build log data
        let log_data = ProtocolMessages::AODV(msg.clone());

        radio::log_handle_message(logger, &hdr, MessageStatus::FORWARDING, None, None, r_type, &msg);

        //Build forward hdr
        let fwd_hdr = hdr
            .create_forward_header(me)
            .set_destination(next_hop)
            .set_payload(serialize_message(msg)?)
            .build();

        Ok(Some((fwd_hdr, log_data)))
    }

    fn process_hello_msg(
        hdr: MessageHeader,
        msg: RouteResponseMessage,
        config: Config,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        me: String,
        r_type: RadioTypes,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        radio::log_handle_message(
            logger,
            &hdr,
            MessageStatus::ACCEPTED,
            Some("HELLO message"),
            None,
            r_type,
            &Messages::HELLO(msg.clone()),
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
        let mut entry = rt
            .entry(hdr.sender.clone())
            .or_insert_with(|| RouteTableEntry::new(&hdr.sender, &me, Some(msg.dest_seq_no)));
        let lifetime = Utc::now()
            + Duration::milliseconds(config.allowed_hello_loss as i64 * HELLO_INTERVAL as i64);
        entry.flags.insert(RTEFlags::ACTIVE_ROUTE);
        entry.lifetime = std::cmp::max(entry.lifetime, lifetime);

        Ok(None)
    }

    fn start_flow(
        dest: String,
        config: Config,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        mut hdr: MessageHeader,
        log_data: ProtocolMessages,
        tx_queue: Sender<Transmission>,
        logger: Logger,
    ) -> Result<(), MeshSimError> {
        let msg_hash = hdr.get_msg_id().to_string();
        let (next_hop, seq_no) = {
            let rt = route_table.lock().expect("Could not lock route table");
            let e = rt.get(&dest).ok_or(MeshSimError {
                kind: MeshSimErrorKind::Worker(format!("No route to {}", &dest)),
                cause: None,
            })?;
            debug!(logger, "Obtained route to {}", &dest);
            let p = e.next_hop.clone();
            (p, e.dest_seq_no)
        };
        //Update header with the next hop
        hdr.destination = next_hop.clone();

        //Record the packet in the data cache
        {
            let mut dc = data_cache.lock().expect("Could not lock data cache");
            dc.insert(
                msg_hash,
                DataCacheEntry {
                    destination: next_hop,
                    seq_no,
                    ts: Utc::now() + Duration::milliseconds(config.next_hop_wait),
                    confirmed: false,
                },
            );
        }

        tx_queue.send((hdr, log_data, Utc::now()))
        .map_err(|e| { 
            let msg = "Failed to queue response into tx_queue".to_string();
            MeshSimError {
                kind: MeshSimErrorKind::Contention(msg),
                cause: Some(Box::new(e)),
            }
        })?;

        Ok(())
    }

    fn start_route_discovery(
        destination: String,
        dest_seq_no: u32,
        rreq_seq_no: Arc<AtomicU32>,
        seq_no: u32, //User must determine if the seq_no needs to be updated
        ttl: Option<usize>,
        retries: Option<usize>,
        repair: bool,
        me: String,
        config: Config,
        tx_queue: Sender<Transmission>,
        pending_routes: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        let rreq_id = rreq_seq_no.fetch_add(1, Ordering::SeqCst) + 1;
        // let mut flags = RREQFlags::GRATUITOUS_RREP; //Why was I doing this?
        let mut flags = Default::default();
        if dest_seq_no == 0 {
            flags |= RREQFlags::UNKNOWN_SEQUENCE_NUMBER;
        }
        let msg = RouteRequestMessage {
            flags,
            hop_count: 0,
            rreq_id,
            destination: destination.clone(),
            dest_seq_no,
            originator: me.clone(),
            orig_seq_no: seq_no,
        };
        //Tag the message
        let msg = Messages::RREQ(msg);
        let log_data = ProtocolMessages::AODV(msg.clone());
        //TTL can be determined by the caller for some specific cases, such as determining
        //connectivity to a neighbour (ttl = 1) or for doing an RREQ in stages.
        let ttl = ttl.unwrap_or(std::usize::MAX);
        let mut hdr = MessageHeader::new(me.clone(), destination.clone(), serialize_message(msg)?);
        hdr.ttl = ttl;

        //Add to the pending-routes list
        {
            let retries = retries.unwrap_or(0);
            let mut pd = pending_routes
                .lock()
                .expect("Could not lock pending routes");
            pd.entry(destination)
                .and_modify(|e| {
                    e.rreq_id = rreq_id;
                    e.lifetime = Utc::now() + Duration::milliseconds(config.path_discovery_time);
                    e.retries = std::cmp::max(e.retries, retries);
                })
                .or_insert(PendingRouteEntry {
                    rreq_id,
                    dest_seq_no,
                    retries,
                    route_repair: repair,
                    lifetime: Utc::now() + Duration::milliseconds(config.path_discovery_time),
                });
        }

        //Add rreq to the cache so that this node does not re-process it
        {
            let mut rr_cache = rreq_cache
                .lock()
                .expect("Error trying to acquire lock on rreq_cache");
            let _cache_entry = rr_cache.insert((me, rreq_id), Utc::now());
        }

        //Broadcast RouteDiscovery
        tx_queue.send((hdr, log_data, Utc::now()))
        .map_err(|e| { 
            let msg = "Failed to queue response into tx_queue".to_string();
            MeshSimError {
                kind: MeshSimErrorKind::Contention(msg),
                cause: Some(Box::new(e)),
            }
        })?;

        /* TODO: This might present a bug in the data analysis. Before, this was logged after calling radio.broadcast(), thus making sure
        the packet had actualley been sent. But now, the packet has been queued. Functionally, it is fine, but from an analysis point of view,
        I'm logging the RREQ started and it doesn't really start until the packet has been sent, which might end up not being sent.
        It's probably fine, but making note of this.
        */ 
        //This log is necessary to differentiate the initial RREQ packet from all the other SENT logs in
        //the intermediate nodes.
        info!(logger, "Initiated RREQ"; "rreq_id" => rreq_id);

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

    fn start_queued_flows(
        queued_transmissions: Arc<Mutex<HashMap<String, Vec<MessageHeader>>>>,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        destination: String,
        _me: String,
        config: Config,
        tx_queue: Sender<Transmission>,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        info!(logger, "Looking for queued flows for {}", &destination);

        let entry: Option<(String, Vec<MessageHeader>)> = {
            let mut qt = queued_transmissions
                .lock()
                .expect("Error trying to acquire lock to transmissions queue");
            qt.remove_entry(&destination)
        };

        if let Some((_key, flows)) = entry {
            info!(logger, "Processing {} queued transmissions.", &flows.len());
            for hdr in flows {
                let dest = destination.clone();
                let log_data = ProtocolMessages::AODV(deserialize_message(hdr.get_payload())?);
                let l = logger.clone();
                let rt = Arc::clone(&route_table);
                let dc = Arc::clone(&data_cache);
                AODV::start_flow(dest, config, rt, dc, hdr, log_data, tx_queue.clone(), l.clone())?;
            }
        }

        Ok(())
    }

    fn route_repair(
        destination: &str,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        rreq_seq_no: Arc<AtomicU32>,
        seq_no: Arc<AtomicU32>,
        me: String,
        config: Config,
        tx_queue: Sender<Transmission>,
        pending_routes: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        logger: &Logger,
    ) -> Result<bool, MeshSimError> {
        let mut rt = route_table.lock().expect("Failed to lock route table");
        if let Some(entry) = rt.get_mut(destination) {
            let current_hop_count = entry.hop_count as usize;
            if current_hop_count > config.max_repair_ttl {
                return Ok(false);
            }

            //Increase the sequence number of the destination to avoid stale routes
            entry.dest_seq_no += 1;

            //Start a new route discovery to the destination
            let ttl = current_hop_count + LOCAL_ADD_TTL;
            let repair = true;
            AODV::start_route_discovery(
                destination.into(),
                entry.dest_seq_no,
                rreq_seq_no,
                seq_no.load(Ordering::SeqCst),
                Some(ttl),
                Some(config.rreq_retries),
                repair,
                me,
                config,
                tx_queue,
                pending_routes,
                rreq_cache,
                logger,
            )?;

            //Mark the route as in repair
            entry.repairing = true;

            return Ok(true);
        }

        Ok(false)
    }

    fn get_self_peer(&self) -> String {
        self.worker_name.clone()
    }

    fn route_table_maintenance(
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
        me: String,
        tx_queue: Sender<Transmission>,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        let mut broken_links = {
            let mut broken_links = HashMap::new();
            let mut rt = route_table.lock().expect("Could not lock route table");
            //Mark expired routes as inactive
            for (_, v) in rt.iter_mut().filter(|(_, v)| {
                v.flags.contains(RTEFlags::ACTIVE_ROUTE) && v.lifetime < Utc::now()
            }) {
                v.flags.remove(RTEFlags::ACTIVE_ROUTE);
                v.lifetime = Utc::now() + Duration::milliseconds(DELETE_PERIOD as i64);
            }

            //Routes to nodes that are broken links (e.g. neighbours) that are not being repaired
            for (k, v) in rt.iter_mut().filter(|(k, v)| {
                !v.flags.contains(RTEFlags::ACTIVE_ROUTE)
                    && v.lifetime < Utc::now()
                    && &v.next_hop == *k
                    && !v.repairing
            }) {
                broken_links.insert(k.clone(), v.dest_seq_no);
            }

            //Delete inactive routes whose lifetime (delete time) has passed and are not being repaired
            rt.retain(|_, v| {
                v.flags.contains(RTEFlags::ACTIVE_ROUTE) || v.lifetime > Utc::now() || v.repairing
            });

            //For debugging purposes, print the RouteTable
            debug!(logger, "RouteTable:");
            for e in rt.iter() {
                debug!(logger, "{:?}", e);
            }
            broken_links
        };

        {
            //Routes for unconfirmed, expired DATA msgs are marked as broken.
            let mut dc = data_cache.lock().expect("Could not lock data_cache");
            for (msg, entry) in dc
                .iter_mut()
                .filter(|(_, v)| !v.confirmed && v.ts < Utc::now())
            {
                info!(
                    logger,
                    "BROKEN_LINK detected";
                    "reason"=>"Unconfirmed DATA message",
                    "destination"=>&entry.destination,
                );

                broken_links.insert(msg.to_string(), entry.seq_no);
                //Mark the msg as confirmed so that we don't process it again or
                //send an RERR for this link due to it again.
                entry.confirmed = true;
            }
        }

        //Affected destinations include those that are no longer reachable due to the detected broken links
        let affected_destinations: HashMap<String, u32> = {
            let rt = route_table.lock().expect("Could not lock route table");
            rt.iter()
                .filter(|(k, v)| {
                    //If any route depends on the broken link as their next hop, they are now unreachable
                    broken_links.contains_key(&v.next_hop) &&
                //but only active routes generate route errors
                v.flags.contains(RTEFlags::ACTIVE_ROUTE) &&
                //and if the unreachable route is to the broken link itself, it generates an RERR only of its precursor list is not empty
                //(e.g. not for routes created by HELLO messages)
                (**k != v.next_hop || !v.precursors.is_empty())
                })
                .map(|(k, v)| (k.clone(), v.dest_seq_no))
                .collect()
        };

        if !affected_destinations.is_empty() {
            // info!(
            //     logger,
            //     "Broken links detected";
            //     "num_broken_links" => broken_links.keys().len(),
            // );

            let rerr_msg = RouteErrorMessage {
                flags: Default::default(),
                destinations: affected_destinations,
            };
            //Tag the message
            let rerr_msg = Messages::RERR(rerr_msg);
            let log_data = ProtocolMessages::AODV(rerr_msg.clone());
            let hdr = MessageHeader::new(me, String::new(), serialize_message(rerr_msg)?);

            tx_queue.send((hdr, log_data, Utc::now()))
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

    fn route_discovery_maintenance(
        rreq_cache: Arc<Mutex<HashMap<(String, u32), DateTime<Utc>>>>,
        pending_routes: Arc<Mutex<HashMap<String, PendingRouteEntry>>>,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        seq_no: Arc<AtomicU32>,
        rreq_no: Arc<AtomicU32>,
        tx_queue: Sender<Transmission>,
        me: String,
        config: Config,
        logger: &Logger,
    ) -> Result<(), MeshSimError> {
        //Has any of the pending routes expired?
        let expired_rreqs: HashMap<String, PendingRouteEntry> = { 
            let pd = pending_routes
            .lock()
            .expect("Could not lock pending routes table");
            pd.iter()
                .filter(|(_, entry)| entry.lifetime < Utc::now())
                .map(|(dest, entry)| (dest.clone(), *entry))
                .collect()
        };

        //We first deal with the RREQs that exhausted their retries
        let failed_rreq_transmissions: Vec<Transmission>= {
            let mut rt = route_table
                .lock()
                .expect("Could not lock route table");

            expired_rreqs
            .iter()
            .filter(|(_, entry)| entry.retries >= config.rreq_retries)
            .filter_map(|(dest, entry)| { 
                info!(
                    logger,
                    "RREQ retries exceeded";
                    "destination"=>dest,
                );

                if entry.route_repair {
                    //Remove the repair flag from the route_entry
                    if let Some(entry) = rt.get_mut(dest) {
                        entry.repairing = false;
                    }
                    //We need to indicate the repair did not succeed and send an RERR
                    let mut destinations = HashMap::new();
                    destinations.insert(dest.into(), entry.dest_seq_no);
                    let msg = Messages::RERR(RouteErrorMessage {
                        destinations,
                        flags: Default::default(),
                    });
                    let log_data = ProtocolMessages::AODV(msg.clone());
                    let payload = match serialize_message(msg) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(logger, "Could not serialise payload: {}", e);
                            return None;
                        },
                    };
                    let hdr = MessageHeader::new(
                        me.clone(),
                        String::from(""),
                        payload,
                    );
                    Some((hdr, log_data, Utc::now()))
                } else {
                    None
                }
            })
            .collect()
        };

        for (hdr, log_data, ts) in failed_rreq_transmissions {
            tx_queue.send((hdr, log_data, ts))
            .map_err(|e| { 
                let msg = "Failed to queue RERR into tx_queue".to_string();
                MeshSimError {
                    kind: MeshSimErrorKind::Contention(msg),
                    cause: Some(Box::new(e)),
                }
            })?;
        }


        let rreqs_to_resend: HashMap<&String, &PendingRouteEntry> = expired_rreqs
            .iter()
            .filter(|(_, entry)| entry.retries < config.rreq_retries)
            .collect();

        //Start a new Route discovery process for each destination with retries remaining.
        for (dest, entry) in rreqs_to_resend {
            let ttl = None;
            let repair = false;
            AODV::start_route_discovery(
                dest.clone(),
                entry.dest_seq_no,
                Arc::clone(&rreq_no),
                seq_no.fetch_add(1, Ordering::SeqCst) + 1,
                ttl,
                Some(entry.retries + 1),
                repair,
                me.clone(),
                config,
                tx_queue.clone(),
                Arc::clone(&pending_routes),
                Arc::clone(&rreq_cache),
                &logger,
            )?;
        }

        //Finally, prune destinations that are stale and have exceeded retries.
        let mut pd = pending_routes
            .lock()
            .expect("Could not lock pending routes table");
        //It's important to re-check retries here, as the broadcast above could fail
        //for RREQs that still have retries available.
        pd.retain(|_, v| v.retries < config.rreq_retries || v.lifetime > Utc::now());

        Ok(())
    }

    fn hello_msg_maintenance(
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        me: String,
        config: Config,
        seq_no: Arc<AtomicU32>,
        tx_queue: Sender<Transmission>,
        last_transmission: Arc<AtomicI64>,
        _logger: &Logger,
    ) -> Result<(), MeshSimError> {
        let num_active_routes = {
            let rt = route_table.lock().expect("Could not lock route table");
            rt.iter()
                .filter(|(_, v)| v.flags.contains(RTEFlags::ACTIVE_ROUTE))
                .count()
        };
        let hello_threshold = Duration::milliseconds(HELLO_INTERVAL as i64)
            .num_nanoseconds()
            .unwrap_or(1_000_000_000);
        if num_active_routes > 0
            && (last_transmission.load(Ordering::SeqCst) + hello_threshold) < Utc::now().timestamp_nanos()
        {
            //Craft HELLO MESSAGE
            let flags: RREPFlags = Default::default();
            let msg = RouteResponseMessage {
                flags,
                prefix_size: 0,
                hop_count: 0,
                destination: me.clone(),
                dest_seq_no: seq_no.load(Ordering::SeqCst),
                originator: String::from("N/A"),
                lifetime: config.allowed_hello_loss as u32 * HELLO_INTERVAL as u32,
            };
            //Tag the message
            let msg = Messages::HELLO(msg);
            let log_data = ProtocolMessages::AODV(msg.clone());
            let hdr = MessageHeader::new(me, String::new(), serialize_message(msg)?);

            tx_queue.send((hdr, log_data, Utc::now()))
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

    fn connectivity_update(
        hdr: &MessageHeader,
        ts: DateTime<Utc>,
        config: Config,
        route_table: Arc<Mutex<HashMap<String, RouteTableEntry>>>,
        data_cache: Arc<Mutex<HashMap<String, DataCacheEntry>>>,
    ) -> Result<(), MeshSimError> {
        //RFC sec. 6.10 - Maintaining local connectivity
        //Passive acknowledgment is used when expecting the next hop to forward a packet.
        //If transmission is not detected within NEXT_HOP_WAIT milliseconds, connectivity
        //can be determined by receiving any packet from the next_hop.
        {
            //If there are data packets pending confirmation for the sender of this packet, confirm them.
            //Otherwise without the passive acknowledgement those links could be deemed broken.
            let mut dc = data_cache.lock().expect("Could not lock data cache");
            dc.iter_mut()
                .filter(|(_k, v)| v.destination == hdr.sender && !v.confirmed)
                .for_each(|(_k, v)| {
                    //If the current message arrived after we logged this data packet, confirm the link.
                    if v.ts - Duration::milliseconds(config.next_hop_wait) < ts {
                        v.confirmed = true
                    }
                });
        }

        //If a route exists to the sender of the current packet, refresh its lifetime.
        //If the route is marked as active, it will extend its lifetime.
        //If the reoute is inactive, it will extend the period of time for which it can be repaired.
        {
            let mut rt = route_table.lock().expect("Could not lock route table");
            let mut entry = rt
                .entry(hdr.sender.clone())
                .or_insert_with(|| RouteTableEntry::new(&hdr.sender, &hdr.sender, None));
            entry.lifetime = std::cmp::max(
                entry.lifetime,
                Utc::now() + Duration::milliseconds(config.active_route_timeout),
            );
        }

        Ok(())
    }

    // fn route_teardown(
    //     dest: &str,
    // ) -> Result<(MessageHeader, ProtocolMessages), MeshSimError> {

    // }
}

fn deserialize_message(data: &[u8]) -> Result<Messages, MeshSimError> {
    from_slice(data).map_err(|e| {
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
