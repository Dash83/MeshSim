//! Protocol that naively routes data by relaying all messages it receives.

use crate::worker::protocols::{Outcome, Protocol, ProtocolMessages};
use crate::worker::radio::{self, *};
use crate::worker::{MessageHeader, MessageStatus};
use crate::{MeshSimError, MeshSimErrorKind};

use chrono::Utc;
use rand::{rngs::StdRng, RngCore};
use serde_cbor::de::*;
use serde_cbor::ser::*;
use slog::{Logger, Record, Serializer, KV};
use std::collections::HashSet;
use std::iter::Iterator;
use std::sync::{Arc, Mutex};

const MSG_CACHE_SIZE: usize = 10000;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct CacheEntry {
    msg_id: String,
}

///The main struct for this protocol. Implements the worker::protocol::Protocol trait.
#[derive(Debug)]
pub struct NaiveRouting {
    worker_name: String,
    worker_id: String,
    msg_cache: Arc<Mutex<HashSet<CacheEntry>>>,
    short_radio: Arc<dyn Radio>,
    rng: Arc<Mutex<StdRng>>,
    logger: Logger,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataMessage {
    payload: Vec<u8>,
}

impl DataMessage {
    pub fn new(payload: Vec<u8>) -> Self {
        DataMessage { payload }
    }
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
        match *self {
            Messages::Data(ref _m) => serializer.emit_str("msg_type", "DATA"),
        }
    }
}

impl Protocol for NaiveRouting {
    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError> {
        //No initialization needed
        Ok(None)
    }

    fn handle_message(
        &self,
        hdr: MessageHeader,
        _r_type: RadioTypes,
    ) -> Result<Outcome, MeshSimError> {
        let msg = deserialize_message(hdr.get_payload()).map_err(|e| {
            let err_msg = String::from("Failed to deserialize payload into a message");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        let msg_cache = Arc::clone(&self.msg_cache);
        let rng = Arc::clone(&self.rng);
        NaiveRouting::handle_message_internal(
            hdr,
            msg,
            self.get_self_peer(),
            msg_cache,
            rng,
            &self.logger,
        )
    }

    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError> {
        let perf_out_queued_start = Utc::now();
        let msg = Messages::Data(DataMessage { payload: data });
        let log_data = ProtocolMessages::Naive(msg.clone());
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

        let tx = self.short_radio.broadcast(hdr.clone())?;
        radio::log_tx(
            &self.logger,
            tx,
            &hdr.msg_id,
            MessageStatus::SENT,
            &hdr.sender,
            &hdr.destination,
            log_data,
        );

        Ok(())
    }

    fn do_maintenance(&self) -> Result<(), MeshSimError> {
        // No maintenance required for this protocol!
        Ok(())
    }
}

impl NaiveRouting {
    /// Creates a new instance of the NaiveRouting protocol handler
    pub fn new(
        worker_name: String,
        worker_id: String,
        sr: Arc<dyn Radio>,
        rng: Arc<Mutex<StdRng>>,
        logger: Logger,
    ) -> NaiveRouting {
        let v = HashSet::with_capacity(MSG_CACHE_SIZE);
        NaiveRouting {
            worker_name,
            worker_id,
            msg_cache: Arc::new(Mutex::new(v)),
            short_radio: sr,
            rng,
            logger,
        }
    }

    fn process_data_message(
        hdr: MessageHeader,
        msg: DataMessage,
        me: String,
        msg_cache: Arc<Mutex<HashSet<CacheEntry>>>,
        rng: Arc<Mutex<StdRng>>,
        logger: &Logger,
    ) -> Result<Outcome, MeshSimError> {
        let msg_id = hdr.get_msg_id().to_string();
        {
            // LOCK:ACQUIRE:MSG_CACHE
            let mut cache = msg_cache.lock().map_err(|_e| {
                let err_msg = String::from("Failed to lock message cache");
                radio::log_handle_message(
                    logger,
                    &hdr,
                    MessageStatus::DROPPED,
                    Some(&err_msg),
                    None,
                    &Messages::Data(msg.clone()),
                );
                MeshSimError {
                    kind: MeshSimErrorKind::Contention(err_msg),
                    // cause: Some(Box::new(e)),
                    cause: None,
                }
            })?;

            let entry = CacheEntry {
                msg_id: msg_id.clone(),
            };
            if cache.contains(&entry) {
                radio::log_handle_message(
                    logger,
                    &hdr,
                    MessageStatus::DROPPED,
                    Some("DUPLICATE"),
                    None,
                    &Messages::Data(msg),
                );
                return Ok((None, None));
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
            cache.insert(CacheEntry { msg_id });
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
            return Ok((None, None));
        }

        let msg = Messages::Data(msg);

        //Message is not meant for this node
        radio::log_handle_message(logger, &hdr, MessageStatus::FORWARDING, None, None, &msg);
        //The payload of the incoming header is still valid, so just build a new header with this node as the sender
        //and leave the rest the same.
        let fwd_hdr = hdr.create_forward_header(me).build();
        //Box the message to log it
        let log_data = ProtocolMessages::Naive(msg);

        Ok((Some(fwd_hdr), Some(log_data)))
    }

    fn handle_message_internal(
        hdr: MessageHeader,
        msg: Messages,
        me: String,
        msg_cache: Arc<Mutex<HashSet<CacheEntry>>>,
        rng: Arc<Mutex<StdRng>>,
        logger: &Logger,
    ) -> Result<Outcome, MeshSimError> {
        match msg {
            Messages::Data(data) => {
                NaiveRouting::process_data_message(hdr, data, me, msg_cache, rng, logger)
            }
        }
    }

    fn get_self_peer(&self) -> String {
        self.worker_name.clone()
    }
}

pub fn deserialize_message(data: &[u8]) -> Result<Messages, MeshSimError> {
    from_slice(data).map_err(|e| {
        let err_msg = String::from("Error deserializing data into message");
        MeshSimError {
            kind: MeshSimErrorKind::Serialization(err_msg),
            cause: Some(Box::new(e)),
        }
    })
}

pub fn serialize_message(msg: Messages) -> Result<Vec<u8>, MeshSimError> {
    to_vec(&msg).map_err(|e| {
        let err_msg = String::from("Error serializing message");
        MeshSimError {
            kind: MeshSimErrorKind::Serialization(err_msg),
            cause: Some(Box::new(e)),
        }
    })
}
