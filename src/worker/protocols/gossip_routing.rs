//! Gossip-based flooding protocol

use crate::worker::protocols::{Transmission, HandleMessageOutcome, Protocol, ProtocolMessages};
use crate::worker::radio::{self, *};
use crate::worker::{MessageHeader, MessageStatus};
use crate::{MeshSimError, MeshSimErrorKind};

use chrono::{DateTime, Utc};
use rand::{rngs::StdRng, Rng, RngCore};
use serde_cbor::de::*;
use serde_cbor::ser::*;
use slog::{Logger, Record, Serializer, KV};
use std::collections::HashSet;
use std::iter::Iterator;
use std::sync::{Arc, Mutex};
use crossbeam_channel::Sender;

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
    k: usize,
    p: f64,
    worker_name: String,
    worker_id: String,
    msg_cache: Arc<Mutex<HashSet<CacheEntry>>>,
    tx_queue: Sender<Transmission>,
    /// RNG used for gossip calculations
    rng: Arc<Mutex<StdRng>>,
    logger: Logger,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataMessage {
    payload: Vec<u8>,
}

/// This enum represents the types of network messages supported in the protocol as well as the
/// data associated with them.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Messages {
    ///Data messages for this protocol.
    Data(DataMessage),
}

impl KV for Messages {
    fn serialize(&self, _rec: &Record, serializer: &mut dyn Serializer) -> slog::Result {
        serializer.emit_str("msg_type", "DATA")
    }
}

impl Protocol for GossipRouting {
    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError> {
        //No initialization needed
        Ok(None)
    }

    fn handle_message(
        &self,
        hdr: MessageHeader,
        _ts: DateTime<Utc>,
        _r_type: RadioTypes,
    ) -> Result<(), MeshSimError> {
        let msg = deserialize_message(hdr.get_payload()).map_err(|e| {
            let err_msg = String::from("Failed to deserialize payload into a message");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        let msg_cache = Arc::clone(&self.msg_cache);
        let rng = Arc::clone(&self.rng);
        let resp = GossipRouting::handle_message_internal(
            hdr,
            msg,
            self.get_self_peer(),
            self.k,
            self.p,
            msg_cache,
            rng,
            &self.logger,
        )?;

        if let Some((resp_hdr, md)) = resp {
            self.tx_queue.send((resp_hdr, md, Utc::now()))
            .map_err(|e| { 
                let msg = format!("Failed to queue response into tx_queue");
                MeshSimError {
                    kind: MeshSimErrorKind::Contention(msg),
                    cause: Some(Box::new(e)),
                }
            })?;
        }

        Ok(())
    }

    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError> {
        let perf_out_queued_start = Utc::now();
        let msg = Messages::Data(DataMessage { payload: data });
        let log_data = ProtocolMessages::Gossip(msg.clone());
        let mut hdr =
            MessageHeader::new(self.get_self_peer(), destination, serialize_message(msg)?);
        hdr.delay = perf_out_queued_start.timestamp_nanos();
        let msg_id = hdr.get_msg_id().to_string();
        info!(&self.logger, "New message"; "msg_id" => &msg_id);
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
            cache.insert(CacheEntry { msg_id });
        }

        self.tx_queue.send((hdr, log_data, Utc::now()))
        .map_err(|e| { 
            let msg = format!("Failed to queue response into tx_queue");
            MeshSimError {
                kind: MeshSimErrorKind::Contention(msg),
                cause: Some(Box::new(e)),
            }
        })?;

        Ok(())
    }

    fn do_maintenance(&self) -> Result<(), MeshSimError> {
        // Gossip requires no maintenance
        Ok(())
    }
}

impl GossipRouting {
    /// Creates a new instance of the NaiveRouting protocol handler
    pub fn new(
        worker_name: String,
        worker_id: String,
        k: usize,
        p: f64,
        tx_queue: Sender<Transmission>,
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
            tx_queue,
            rng,
            logger,
        }
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
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        {
            // LOCK:ACQUIRE:MSG_CACHE
            let mut cache = msg_cache.lock().map_err(|_e| {
                let err_msg = String::from("Failed to lock message cache");

                radio::log_handle_message(
                    logger,
                    &hdr,
                    MessageStatus::DROPPED,
                    None,
                    None,
                    &Messages::Data(msg.clone()),
                );
                MeshSimError {
                    kind: MeshSimErrorKind::Contention(err_msg),
                    // cause: Some(Box::new(e.clone())),
                    cause: None,
                }
            })?;

            for entry in cache.iter() {
                if entry.msg_id == hdr.get_msg_id() {
                    radio::log_handle_message(
                        logger,
                        &hdr,
                        MessageStatus::DROPPED,
                        Some("DUPLICATE"),
                        None,
                        &Messages::Data(msg),
                    );
                    return Ok(None);
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
            cache.insert(CacheEntry {
                msg_id: hdr.get_msg_id().to_string(),
            });
        } // LOCK:RELEASE:MSG_CACHE

        //Check if this node is the intended recipient of the message.
        if hdr.destination == me {
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::ACCEPTED,
                None,
                None,
                &Messages::Data(msg),
            );
            return Ok(None);
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
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::DROPPED,
                Some("Gossip failed"),
                None,
                &Messages::Data(msg),
            );
            //Not gossiping this message.
            return Ok(None);
        }

        let msg = Messages::Data(msg);
        radio::log_handle_message(
            logger,
            &hdr,
            MessageStatus::FORWARDING,
            Some("Gossip succeeded"),
            None,
            &msg,
        );
        //The payload of the incoming header is still valid, so just build a new header with this node as the sender
        //and leave the rest the same.
        let fwd_hdr = hdr.create_forward_header(me).build();
        //Box the message to log it
        let log_data = ProtocolMessages::Gossip(msg);

        Ok(Some((fwd_hdr, log_data)))
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
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        match msg {
            Messages::Data(data) => {
                GossipRouting::process_data_message(hdr, data, k, p, me, msg_cache, rng, logger)
            }
        }
    }

    fn get_self_peer(&self) -> String {
        self.worker_name.clone()
    }
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
