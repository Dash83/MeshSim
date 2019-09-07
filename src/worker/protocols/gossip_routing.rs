//! Gossip-based flooding protocol

use crate::worker::protocols::Protocol;
use crate::worker::radio::*;
use crate::worker::{MessageHeader, Peer};
use crate::{MeshSimError, MeshSimErrorKind};
use md5::Digest;
use serde_cbor::de::*;
use serde_cbor::ser::*;
use slog::Logger;
use std::sync::{Arc, Mutex};
use rand::{rngs::StdRng, Rng};

const MSG_CACHE_SIZE: usize = 2000;
/// The default number of hops messages are guaranteed to be propagated
pub const DEFAULT_MIN_HOPS: usize = 2;
/// The default gossip-probability value
pub const DEFAULT_GOSSIP_PROB: f64 = 0.70;

#[derive(Debug)]
struct CacheEntry {
    msg_id: Digest,
}

///The main struct for this protocol. Implements the worker::protocol::Protocol trait.
#[derive(Debug)]
pub struct GossipRouting {
    k : usize,
    p : f64,
    worker_name: String,
    worker_id: String,
    msg_cache: Arc<Mutex<Vec<CacheEntry>>>,
    short_radio: Arc<dyn Radio>,
    /// RNG used for gossip calculations
    rng: Arc<Mutex<StdRng>>,
    logger: Logger,
}

/// This enum represents the types of network messages supported in the protocol as well as the
/// data associated with them.
#[derive(Debug, Serialize, Deserialize)]
pub enum Messages {
    ///Data messages for this protocol.
    Data(Vec<u8>),
}

impl Protocol for GossipRouting {
    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError> {
        //No initialization needed
        Ok(None)
    }

    fn handle_message(
        &self,
        mut header: MessageHeader,
        _r_type: RadioTypes,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
        let msg_hash = header.get_hdr_hash();

        let data = match header.payload.take() {
            Some(d) => d,
            None => {
                warn!(
                    self.logger,
                    "Messaged received from {:?} had empty payload.", header.sender
                );
                return Ok(None);
            }
        };

        let msg = GossipRouting::build_protocol_message(data).map_err(|e| {
            let err_msg = String::from("Failed to deserialize payload into a message");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        let msg_cache = Arc::clone(&self.msg_cache);
        let rng = Arc::clone(&self.rng);
        GossipRouting::handle_message_internal(
            header,
            msg,
            self.get_self_peer(),
            msg_hash,
            self.k,
            self.p,
            msg_cache,
            rng,
            &self.logger,
        )
    }

    fn send(&self, destination: String, data: Vec<u8>) -> Result<(), MeshSimError> {
        let mut dest = Peer::new();
        dest.name = destination;

        let hdr = GossipRouting::create_data_message(self.get_self_peer(), dest, 1, data)?;
        let msg_hash = hdr.get_hdr_hash();
        {
            let mut cache = self.msg_cache.lock().expect("Could not lock message cache");
            //Have not seen this message yet.
            //Is there space in the cache?
            if cache.len() >= MSG_CACHE_SIZE {
                let _res = cache.remove(0);
            }
            //Log message
            cache.push(CacheEntry { msg_id: msg_hash });
        }

        self.short_radio.broadcast(hdr)?;
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
        let v = Vec::new();
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
        sender: Peer,
        destination: Peer,
        hops: u16,
        data: Vec<u8>,
    ) -> Result<MessageHeader, MeshSimError> {
        let data_msg = Messages::Data(data);
        let payload = to_vec(&data_msg).map_err(|e| {
            let err_msg = String::from("Failed to serialize message");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        //info!("Built DATA message for peer: {}, id {:?}", &destination.name, destination.id);

        //Build the message header that's ready for sending.
        let msg = MessageHeader {
            sender,
            destination,
            hops,
            delay: 0u64,
            payload: Some(payload),
        };
        Ok(msg)
    }

    fn process_data_message(
        hdr: MessageHeader,
        data: Vec<u8>,
        msg_hash: Digest,
        k: usize,
        p: f64,
        me: Peer,
        msg_cache: Arc<Mutex<Vec<CacheEntry>>>,
        rng: Arc<Mutex<StdRng>>,
        logger: &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
        info!(
            logger,
            "Received DATA message {:x} from {}", &msg_hash, &hdr.sender.name
        );

        {
            // LOCK:ACQUIRE:MSG_CACHE
            let mut cache = match msg_cache.lock() {
                Ok(guard) => guard,
                Err(e) => {
                    error!(logger, "Failed to lock message cache: {}", e);
                    return Ok(None);
                }
            };

            for entry in cache.iter() {
                if entry.msg_id == msg_hash {
                    info!(logger, "Dropping repeated message {:x}", &msg_hash);
                    return Ok(None);
                }
            }

            //Have not seen this message yet.
            //Is there space in the cache?
            if cache.len() >= MSG_CACHE_SIZE {
                let _res = cache.remove(0);
            }
            //Log message
            cache.push(CacheEntry { msg_id: msg_hash });
        } // LOCK:RELEASE:MSG_CACHE

        //Check if this node is the intended recipient of the message.
        if hdr.destination.name == me.name {
            info!(logger, "Message {:x} reached its destination", &msg_hash; "route_length" => hdr.hops);
            return Ok(None);
        }

        //Gossip?
        let s: f64 = {
            let mut rng = rng.lock().expect("Could not obtain lock for RNG");
            rng.gen_range(0f64, 1f64)
        };
        debug!(logger, "Gossip prob {}", s);
        if hdr.hops as usize > k && s > p {
            info!(logger, "Not forwarding the message");
            //Not gossiping this message.
            return Ok(None);
        }

        let response = GossipRouting::create_data_message(me, hdr.destination, hdr.hops + 1, data)?;

        Ok(Some(response))
    }

    fn handle_message_internal(
        hdr: MessageHeader,
        msg: Messages,
        me: Peer,
        msg_hash: Digest,
        k: usize,
        p: f64,
        msg_cache: Arc<Mutex<Vec<CacheEntry>>>,
        rng: Arc<Mutex<StdRng>>,
        logger: &Logger,
    ) -> Result<Option<MessageHeader>, MeshSimError> {
        match msg {
            Messages::Data(data) => {
                GossipRouting::process_data_message(
                    hdr,
                    data,
                    msg_hash,
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

    fn build_protocol_message(data: Vec<u8>) -> Result<Messages, serde_cbor::Error> {
        let res: Result<Messages, serde_cbor::Error> = from_slice(data.as_slice());
        res
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
