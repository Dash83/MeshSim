//! Protocol that naively routes data by relaying all messages it receives.
//! 

extern crate serde_cbor;
extern crate rand;
extern crate md5;

use worker::protocols::Protocol;
use worker::{WorkerError, Peer, MessageHeader, AddressType};
use worker::radio::*;
use std::sync::{Arc, Mutex};
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;
use self::rand::{StdRng, Rng};
use self::md5::Digest;

const MSG_CACHE_SIZE : usize = 200;

#[derive(Debug)]
struct CacheEntry {
    sender : String,
    msg_id : Digest,
}

///The main struct for this protocol. Implements the worker::protocol::Protocol trait.
#[derive(Debug)]
pub struct NaiveRouting {
    worker_name : String,
    worker_id : String,
    msg_cache : Arc<Mutex<Vec<CacheEntry>>>,
    short_radio : Arc<Radio>,
}

/// This enum represents the types of network messages supported in the protocol as well as the
/// data associated with them.
#[derive(Debug, Serialize, Deserialize)]
pub enum Messages {
    ///Data messages for this protocol. 
    Data(Vec<u8>),
}

impl Protocol for NaiveRouting {
    fn init_protocol(&self) -> Result<Option<MessageHeader>, WorkerError> {
        //No initialization needed
        Ok(None)
    }

    fn handle_message(&self, mut header : MessageHeader, r_type : RadioTypes) -> Result<Option<MessageHeader>, WorkerError> {
        let msg_hash = header.get_hdr_hash()?;

        let data = match header.payload.take() {
            Some(d) => { d },
            None => {
                warn!("Messaged received from {:?} had empty payload.", header.sender);
                return Ok(None)
            }
        };

        let msg = NaiveRouting::build_protocol_message(data)?;
        let msg_cache = Arc::clone(&self.msg_cache);
        NaiveRouting::handle_message_internal(header, msg, self.get_self_peer(), msg_hash, msg_cache)
    }

    fn send(&self, destination : String, data : Vec<u8>) -> Result<(), WorkerError> {
        let mut dest = Peer::new();
        dest.name = destination;
        let msg = Messages::Data(data);
        let payload = to_vec(&msg)?;
        let hdr = MessageHeader{ sender : self.get_self_peer(),
                                 destination : dest,
                                 payload : Some(payload) };
        let _res = self.short_radio.broadcast(hdr)?;
        Ok(())
    }

}

impl NaiveRouting {
    /// Creates a new instance of the NaiveRouting protocol handler
    pub fn new(worker_name : String, worker_id : String, sr : Arc<Radio> ) -> NaiveRouting {
        let v = Vec::new();
        NaiveRouting{ worker_name : worker_name,
                      worker_id : worker_id,
                      msg_cache : Arc::new(Mutex::new(v)),
                      short_radio : sr }
    }

    fn create_data_message(sender : Peer, destination : Peer, data : Vec<u8>) -> Result<MessageHeader, WorkerError> {
        let data_msg = Messages::Data(data);
        let payload = to_vec(&data_msg)?;
        //info!("Built DATA message for peer: {}, id {:?}", &destination.name, destination.id);
        
        //Build the message header that's ready for sending.
        let msg = MessageHeader{ sender : sender, 
                                 destination : destination, 
                                 payload : Some(payload) };
        Ok(msg)
    }

    fn process_data_message(hdr : MessageHeader, 
                            data : Vec<u8>, 
                            msg_hash : Digest, 
                            me : Peer,
                            msg_cache : Arc<Mutex<Vec<CacheEntry>>> ) -> Result<Option<MessageHeader>, WorkerError> {
        info!("Received DATA message {:x} from {}", &msg_hash, &hdr.sender.name);

        { // LOCK:ACQUIRE:MSG_CACHE
            let mut cache = match msg_cache.lock() {
                Ok(guard) => guard,
                Err(e) => {
                    error!("Failed to lock message cache: {}", e);
                    return Ok(None)
                }
            };
            
            for entry in cache.iter() {
                if entry.sender == hdr.sender.id && entry.msg_id == msg_hash {
                    info!("Dropping repeated message {:x}", &msg_hash);
                    return Ok(None)
                }
            }

            //Have not seen this message yet.
            //Is there space in the cache?
            if cache.len() >= MSG_CACHE_SIZE {
                let _res = cache.remove(0);
            }
            //Log message
            cache.push(CacheEntry{ sender : hdr.sender.id, msg_id : msg_hash});

        } // LOCK:RELEASE:MSG_CACHE

        //Check if this node is the intenteded recipient of the message.
        if hdr.destination.name == me.name {
            info!("Message {:x} reached its destination", &msg_hash);
            return Ok(None)
        }

        let response = NaiveRouting::create_data_message(me, hdr.destination, data)?;

        Ok(Some(response))
    }

    fn handle_message_internal(hdr : MessageHeader, 
                               msg : Messages, 
                               me : Peer, 
                               msg_hash : Digest,
                               msg_cache : Arc<Mutex<Vec<CacheEntry>>>) -> Result<Option<MessageHeader>, WorkerError> {
        let response = match msg {
                    Messages::Data(data) => {
                        NaiveRouting::process_data_message(hdr, data, msg_hash, me, msg_cache)
                    },
                };
        response
    }

    fn build_protocol_message(data : Vec<u8>) -> Result<Messages, serde_cbor::Error> {
        let res : Result<Messages, serde_cbor::Error> = from_slice(data.as_slice());
        res
    }

    fn get_self_peer(&self) -> Peer {
        Peer{ name : self.worker_name.clone(),
              id : self.worker_id.clone(),
              addresses : vec![ AddressType::ShortRange(String::from(self.short_radio.get_address())) ] }
    }
}