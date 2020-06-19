//! Gossip-based flooding protocol

use crate::worker::protocols::{Protocol, Outcome};
use crate::worker::radio::{self, *};
use crate::worker::{MessageHeader, Peer, MessageStatus};
use crate::{MeshSimError, MeshSimErrorKind};
use md5::Digest;
use serde_cbor::de::*;
use serde_cbor::ser::*;
use slog::{Logger,KV, Record, Serializer};
use std::sync::{Arc, Mutex};
use std::collections::HashSet;
use rand::{rngs::StdRng, RngCore, Rng};
use std::iter::Iterator;

const MSG_CACHE_SIZE: usize = 10000;
/// The default number of hops messages are guaranteed to be propagated
pub const DEFAULT_MIN_HOPS: usize = 2;
/// The default gossip-probability value
pub const DEFAULT_GOSSIP_PROB: f64 = 0.70;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct CacheEntry {
    msg_id: String,
}

///The main struct for this protocol. Implements the worker::protocol::Protocol trait.
#[derive(Debug)]
pub struct GossipRouting {
    k : usize,
    p : f64,
    worker_name: String,
    worker_id: String,
    msg_cache: Arc<Mutex<HashSet<CacheEntry>>>,
    short_radio: Arc<dyn Radio>,
    /// RNG used for gossip calculations
    rng: Arc<Mutex<StdRng>>,
    logger: Logger,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
struct DataMessage { 
    payload : Vec<u8>,
}

impl KV for DataMessage {
    fn serialize(&self, _rec: &Record, serializer: &mut dyn Serializer) -> slog::Result {
        serializer.emit_str("msg_type", "DATA")
    }
}

/// This enum represents the types of network messages supported in the protocol as well as the
/// data associated with them.
#[derive(Debug, Serialize, Deserialize)]
enum Messages {
    ///Data messages for this protocol.
    Data(DataMessage),
}

impl Protocol for GossipRouting {
    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError> {
        //No initialization needed
        Ok(None)
    }

    fn handle_message(
        &self,
        mut hdr: MessageHeader,
        _r_type: RadioTypes,
    ) -> Result<Outcome, MeshSimError> {
        let msg = GossipRouting::build_protocol_message(hdr.get_payload()).map_err(|e| {
            let err_msg = String::from("Failed to deserialize payload into a message");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        let msg_cache = Arc::clone(&self.msg_cache);
        let rng = Arc::clone(&self.rng);
        GossipRouting::handle_message_internal(
            hdr,
            msg,
            self.get_self_peer(),
            self.k,
            self.p,
            msg_cache,
            rng,
            &self.logger,
        )
    }

    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError> {
        let hdr = GossipRouting::create_data_message(self.get_self_peer(), destination, 1, data)?;
        let msg_id = hdr.get_msg_id().to_string();
        {
            let mut cache = self.msg_cache.lock().expect("Could not lock message cache");
            //Have not seen this message yet.
            //Is there space in the cache?
            if cache.len() >= MSG_CACHE_SIZE {
                let mut rng = self.rng.lock().expect("Could not lock RNG");
                let r = rng.next_u64() as usize % cache.len();
                let e = cache.iter().nth(r).unwrap().clone();
                cache.remove(&e);
            }
            //Log message
            cache.insert(CacheEntry { msg_id: msg_id.clone() });
        }

        let tx = self.short_radio.broadcast(hdr)?;
        let md = MessageMetadata::new(msg_id, "DATA", MessageStatus::SENT);
        radio::log_tx(&self.logger, tx, md);

        Ok(())
    }
}

impl GossipRouting {
    /// Creates a new instance of the NaiveRouting protocol handler
    pub fn new(
        worker_name: String,
        worker_id: String,
        k : usize,
        p : f64,
        sr: Arc<dyn Radio>,
        rng: Arc<Mutex<StdRng>>,
        logger: Logger,
    ) -> GossipRouting {
        let v = HashSet::with_capacity(MSG_CACHE_SIZE);
        GossipRouting {
            worker_name,
            worker_id,
            k,
            p,
            msg_cache: Arc::new(Mutex::new(v)),
            short_radio: sr,
            rng,
            logger,
        }
    }

    fn create_data_message(
        sender: String,
        destination: String,
        hops: u16,
        data: Vec<u8>,
    ) -> Result<MessageHeader, MeshSimError> {
        let data_msg = Messages::Data(DataMessage{payload : data});
        let payload = to_vec(&data_msg).map_err(|e| {
            let err_msg = String::from("Failed to serialize message");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        //info!("Built DATA message for peer: {}, id {:?}", &destination, destination.id);

        //Build the message header that's ready for sending.
        let msg = MessageHeader::new(
            sender,
            destination,
            payload,
            hops,
        );
        Ok(msg)
    }

    fn process_data_message(
        hdr: MessageHeader,
        msg: DataMessage,
        k: usize,
        p: f64,
        me: String,
        msg_cache: Arc<Mutex<HashSet<CacheEntry>>>,
        rng: Arc<Mutex<StdRng>>,
        logger: &Logger,
    ) -> Result<Outcome, MeshSimError> {
        {
            // LOCK:ACQUIRE:MSG_CACHE
            let mut cache = msg_cache
                .lock()
                .map_err(|e| {
                    let err_msg = String::from("Failed to lock message cache");
                    // error!(
                    //     logger,
                    //     "Received message";
                    //     "msg_id" => &msg_id,
                    //     "msg_type"=> "DATA",
                    //     "sender"=> &hdr.sender,
                    //     "status"=> MessageStatus::DROPPED,
                    //     "reason"=> &err_msg,
                    // );
                    radio::log_rx(
                        logger, 
                        &hdr,
                        MessageStatus::DROPPED,
                        None,
                        None,
                        &msg,
                    );
                    MeshSimError {
                        kind: MeshSimErrorKind::Contention(err_msg),
                        // cause: Some(Box::new(e.clone())),
                        cause: None,
                    }
            })?; 

            for entry in cache.iter() {
                if entry.msg_id == hdr.get_msg_id() {
                    // info!(
                    //     logger,
                    //     "Received message";
                    //     "msg_id" => &msg_id,
                    //     "msg_type"=>"DATA",
                    //     "sender"=>&hdr.sender,
                    //     "status"=>MessageStatus::DROPPED,
                    //     "reason"=>"DUPLICATE",
                    //     "sender"=>&hdr.sender,
                    // );
                    radio::log_rx(
                        logger, 
                        &hdr,
                        MessageStatus::DROPPED,
                        Some("DUPLICATE"),
                        None,
                        &msg,
                    );
                    return Ok((None, None));
                }
            }

            //Have not seen this message yet.
            //Is there space in the cache?
            if cache.len() >= MSG_CACHE_SIZE {
                warn!(logger, "Message cache full");
                let mut rng = rng.lock().expect("Could not lock RNG");
                let r = rng.next_u64() as usize % cache.len();
                let e = cache.iter().nth(r).unwrap().clone();
                cache.remove(&e);
            }
            //Log message
            cache.insert(CacheEntry { msg_id: hdr.get_msg_id().to_string() });
        } // LOCK:RELEASE:MSG_CACHE

        //Check if this node is the intended recipient of the message.
        if hdr.destination == me {
            // info!(
            //     logger,
            //     "Received message";
            //     "msg_id" => hdr.get_msg_id(),
            //     "msg_type"=>"DATA",
            //     "sender"=>&hdr.sender,
            //     "status"=>MessageStatus::ACCEPTED,
            //     "route_length" => hdr.hops
            // );
            radio::log_rx(
                logger, 
                &hdr,
                MessageStatus::ACCEPTED,
                None,
                None,
                &msg
            );
            return Ok((None, None));
        }

        //Gossip?
        let s: f64 = {
            let mut rng = rng.lock().expect("Could not obtain lock for RNG");
            rng.gen_range(0f64, 1f64)
        };
        debug!(logger, "Gossip prob {}", s);
        if hdr.hops as usize > k && s > p {
            // info!(
            //     logger,
            //     "Received message";
            //     "msg_id" => hdr.get_msg_id(),
            //     "msg_type"=>"DATA",
            //     "sender"=>&hdr.sender,
            //     "status"=>MessageStatus::DROPPED,
            //     "reason"=>"GOSSIP",
            // );
            radio::log_rx(
                logger, 
                &hdr,
                MessageStatus::DROPPED,
                Some("Gossip failed"),
                None,
                &msg
            );
            //Not gossiping this message.
            return Ok((None, None));
        }

        radio::log_rx(
            logger, 
            &hdr,
            MessageStatus::FORWARDING,
            Some("Gossip succeeded"),
            None,
            &msg
        );
        let response = GossipRouting::create_data_message(me, hdr.destination.clone(), hdr.hops + 1, msg.payload)?;
        let mut md = MessageMetadata::new(response.get_msg_id().to_string(), "DATA", MessageStatus::FORWARDING);
        // let sender = hdr.sender.clone();
        // md.source = Some(&sender);
        // info!(
        //     logger,
        //     "Received message {:x}", &msg_hash;
        //     "msg_type"=>"DATA",
        //     "sender"=>&hdr.sender,
        //     "status"=>MessageStatus::FORWARDED,
        // );

        Ok((Some(response), Some(md)))
    }

    fn handle_message_internal(
        hdr: MessageHeader,
        msg: Messages,
        me: String,
        k: usize,
        p: f64,
        msg_cache: Arc<Mutex<HashSet<CacheEntry>>>,
        rng: Arc<Mutex<StdRng>>,
        logger: &Logger,
    ) -> Result<Outcome, MeshSimError> {
        match msg {
            Messages::Data(data) => {
                GossipRouting::process_data_message(
                    hdr,
                    data,
                    k,
                    p,
                    me,
                    msg_cache,
                    rng,
                    logger
                )
            }
        }
    }

    fn build_protocol_message(data: &[u8]) -> Result<Messages, serde_cbor::Error> {
        let res: Result<Messages, serde_cbor::Error> = from_slice(data);
        res
    }

    fn get_self_peer(&self) -> String {
        self.worker_name.clone()
    }
}
