//! Protocol that naively routes data by relaying all messages it receives.

use crate::worker::protocols::{Outcome, Protocol};
use crate::worker::radio::{self, *};
use crate::worker::{MessageHeader, MessageStatus, Peer};
use crate::{MeshSimError, MeshSimErrorKind};
use md5::Digest;
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
struct DataMessage {
    payload: Vec<u8>,
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

impl Protocol for NaiveRouting {
    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError> {
        //No initialization needed
        Ok(None)
    }

    fn handle_message(
        &self,
        mut hdr: MessageHeader,
        _r_type: RadioTypes,
    ) -> Result<Outcome, MeshSimError> {
        let msg = NaiveRouting::build_protocol_message(hdr.get_payload()).map_err(|e| {
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
        let msg = DataMessage { payload: data };
        let log_data = Box::new(msg.clone());
        let hdr = MessageHeader::new(
            self.get_self_peer(),
            destination,
            serialize_message(Messages::Data(msg))?,
        );
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
            cache.insert(CacheEntry {
                msg_id: msg_id.clone(),
            });
        }

        self.short_radio.broadcast(hdr, log_data)?;

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

    // fn create_data_message(
    //     sender: String,
    //     destination: String,
    //     hops: u16,
    //     data: Vec<u8>,
    // ) -> Result<MessageHeader, MeshSimError> {
    //     let data_msg = Messages::Data(DataMessage{ payload : data});
    //     let payload = to_vec(&data_msg).map_err(|e| {
    //         let err_msg = String::from("Failed to serialize message");
    //         MeshSimError {
    //             kind: MeshSimErrorKind::Serialization(err_msg),
    //             cause: Some(Box::new(e)),
    //         }
    //     })?;
    //     //info!("Built DATA message for peer: {}, id {:?}", &destination, destination.id);

    //     //Build the message header that's ready for sending.
    //     let msg = MessageHeader::new(
    //         sender,
    //         destination,
    //         payload,
    //         hops,
    //     );
    //     Ok(msg)
    // }

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
            let mut cache = msg_cache.lock().map_err(|e| {
                let err_msg = String::from("Failed to lock message cache");
                radio::log_rx(
                    logger,
                    &hdr,
                    MessageStatus::DROPPED,
                    Some(&err_msg),
                    None,
                    &msg,
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
                msg_id: msg_id.clone(),
            });
        } // LOCK:RELEASE:MSG_CACHE

        //Check if this node is the intended recipient of the message.
        if hdr.destination == me {
            radio::log_rx(logger, &hdr, MessageStatus::ACCEPTED, None, None, &msg);
            return Ok((None, None));
        }

        //Message is not meant for this node
        radio::log_rx(logger, &hdr, MessageStatus::FORWARDING, None, None, &msg);
        //The payload of the incoming header is still valid, so just build a new header with this node as the sender
        //and leave the rest the same.
        let fwd_hdr = hdr.create_forward_header(me.clone()).build();
        //Box the message to log it
        let log_data = Box::new(msg);

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

    fn build_protocol_message(data: &[u8]) -> Result<Messages, serde_cbor::Error> {
        let res: Result<Messages, serde_cbor::Error> = from_slice(data);
        res
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
