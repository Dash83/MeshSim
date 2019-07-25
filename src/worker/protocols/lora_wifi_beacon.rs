//! Module for the test protocol that uses Lora and Wifi radios

extern crate rand;
extern crate slog;
extern crate serde_cbor;
extern crate md5;

use worker::protocols::Protocol;
use worker::{WorkerError, Peer, MessageHeader};
use worker::radio::{RadioTypes, Radio};
use std::sync::Arc;
use self::rand::{rngs::StdRng, SeedableRng};
use self::slog::Logger;
use std::time::Duration;
use std::thread;
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;
use self::md5::Digest;

const WIFI_BEACON_TIMEOUT : u64 = 1_000;
const LORA_BEACON_TIMEOUT : u64 = 1_000;

///Base structure to hold the state of the LoraWifiBeacon protocol
#[derive(Debug)]
pub struct LoraWifiBeacon {
    worker_name : String,
    worker_id : String,
    wifi_radio : Arc<Radio>,
    lora_radio : Arc<Radio>,
//    rng : StdRng,
    logger : Logger,
}

///Messages used in this protocol
#[derive(Debug, Serialize, Deserialize)]
enum Messages {
    Beacon(u64),
}

impl Protocol for LoraWifiBeacon {
    fn handle_message(
        &self,
        mut hdr : MessageHeader,
        r_type : RadioTypes
    ) -> Result<Option<MessageHeader>, WorkerError> {
        let msg_hash = hdr.get_hdr_hash()?;

        let data = match hdr.payload.take() {
            Some(d) => { d },
            None => {
                warn!(self.logger, "Messaged received from {:?} had empty payload", hdr.sender);
                return Ok(None)
            }
        };

        let msg = LoraWifiBeacon::build_protocol_message(data)?;
        let self_peer = self.get_self_peer();
        let wifi_radio = Arc::clone(&self.wifi_radio);
        let lora_radio = Arc::clone(&self.lora_radio);
        let logger = self.logger.clone();
        let link = match r_type {
            RadioTypes::ShortRange => String::from("wifi"),
            RadioTypes::LongRange => String::from("lora"),
        };
        LoraWifiBeacon::handle_message_internal(hdr,
                                                msg,
                                                link,
                                                self_peer,
                                                msg_hash,
                                                wifi_radio,
                                                lora_radio,
                                                logger)
    }

    /// Function to initialize the protocol.
    fn init_protocol(&self) -> Result<Option<MessageHeader>, WorkerError> {
        let wifi_radio = Arc::clone(&self.wifi_radio);
        let self_peer = self.get_self_peer();
        let logger = self.logger.clone();
        let link = String::from("wifi");
        let wifi_beacon_handle = thread::spawn(move || {
            LoraWifiBeacon::beacon_loop(wifi_radio,
                                        WIFI_BEACON_TIMEOUT,
                                        self_peer,
                                        link,
                                        logger);
        });

        let lora_radio = Arc::clone(&self.lora_radio);
        let self_peer = self.get_self_peer();
        let logger = self.logger.clone();
        let link = String::from("lora");
        let lora_beacon_handle = thread::spawn(move || {
            LoraWifiBeacon::beacon_loop(lora_radio,
                                        LORA_BEACON_TIMEOUT,
                                        self_peer,
                                        link,
                                        logger);
        });
        Ok(None)
    }

    /// Function to send command to another node in the network
    fn send(&self, destination : String, data : Vec<u8>) -> Result<(), WorkerError> {
        unimplemented!("LoraWifiBeacon does not support send commands");
    }
}

impl LoraWifiBeacon {
    ///Creates a new instance of the LoraWifi protocol
    pub fn new(worker_name : String,
               worker_id : String,
               wifi_radio : Arc<Radio>,
               lora_radio : Arc<Radio>,
//               rng : StdRng,
               logger : Logger) -> LoraWifiBeacon {
        LoraWifiBeacon{ worker_name: worker_name,
                        worker_id : worker_id,
                        wifi_radio : wifi_radio,
                        lora_radio : lora_radio,
//                        rng : rng,
                        logger: logger }
    }

    fn beacon_loop(radio : Arc<Radio>,
                   timeout : u64,
                   self_peer : Peer,
                   link : String,
                   logger : Logger) -> Result<(), WorkerError> {
        let mut counter : u64 = 0;
        let sleep_time = Duration::from_millis(timeout);

        loop {
            thread::sleep(sleep_time);
            counter += 1;
            let msg = Messages::Beacon(counter);
            let mut hdr = MessageHeader::new();
            hdr.sender = self_peer.clone();
            hdr.payload = Some(to_vec(&msg)?);

            let _res = radio.broadcast(hdr)?;
            info!(logger, "Beacon sent over {}:{}", &link, counter);
        }
    }

    fn get_self_peer(&self) -> Peer {
        Peer{ name : self.worker_name.clone(),
            id : self.worker_id.clone(),
            short_address : None,
            long_address : None, }
    }

    fn build_protocol_message(data : Vec<u8>) -> Result<Messages, serde_cbor::Error> {
        let res : Result<Messages, serde_cbor::Error> = from_slice(data.as_slice());
        res
    }

    fn handle_message_internal(
        hdr : MessageHeader,
        msg : Messages,
        link : String,
        self_peer : Peer,
        msg_hash : Digest,
        wifi_radio : Arc<Radio>,
        lora_radio : Arc<Radio>,
        logger : Logger,
    ) -> Result<Option<MessageHeader>, WorkerError> {
        match msg {
            Messages::Beacon(counter) => {
                LoraWifiBeacon::process_beacon_msg(hdr, counter, link, logger)
            }
        }
    }

    fn process_beacon_msg(
        mut hdr : MessageHeader,
        counter : u64,
        link : String,
        logger : Logger,
    ) -> Result<Option<MessageHeader>, WorkerError> {
        info!(logger, "Beacon received over {} from {}:{}", link, hdr.sender.name, counter);
        Ok(None)
    }
}