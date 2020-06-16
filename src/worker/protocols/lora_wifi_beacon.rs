//! Module for the test protocol that uses Lora and Wifi radios

use crate::worker::protocols::{Protocol, Outcome};
use crate::worker::radio::{Radio, RadioTypes};
use crate::worker::{MessageHeader, Peer, MessageStatus};
use crate::{MeshSimError, MeshSimErrorKind};
use md5::Digest;
use rand::thread_rng;
use rand::RngCore;
use serde_cbor::de::*;
use serde_cbor::ser::*;
use slog::Logger;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use rand::{rngs::StdRng, Rng};

const WIFI_BEACON_TIMEOUT: u64 = 2_000;
const LORA_BEACON_TIMEOUT: u64 = 2_000;

///Base structure to hold the state of the LoraWifiBeacon protocol
#[derive(Debug)]
pub struct LoraWifiBeacon {
    worker_name: String,
    worker_id: String,
    wifi_radio: Arc<dyn Radio>,
    lora_radio: Arc<dyn Radio>,
    rng : StdRng,
    logger: Logger,
}

///Messages used in this protocol
#[derive(Debug, Serialize, Deserialize)]
enum Messages {
    Beacon(u64),
    BeaconResponse(u64),
}

impl Protocol for LoraWifiBeacon {
    fn handle_message(
        &self,
        mut hdr: MessageHeader,
        r_type: RadioTypes,
    ) -> Result<Outcome, MeshSimError> {
        let msg_id = hdr.get_hdr_hash();

        let data = match hdr.payload.take() {
            Some(d) => d,
            None => {
                warn!(
                    self.logger,
                    "Received message";
                    "msg_id" => &msg_id,
                    "msg_type"=> "UNKNOWN",
                    "sender"=> &hdr.sender.name,
                    "status"=> MessageStatus::DROPPED,
                    "reason"=> "Message has empty payload"
                );
                return Ok((None, None));
            }
        };

        //Filter out packets coming from this node, as we get many from the multicast.
        if hdr.sender.name == self.worker_name {
            return Ok((None, None));
        }

        let msg = LoraWifiBeacon::deserialize_message(data)?;
        let self_peer = self.get_self_peer();
        let wifi_radio = Arc::clone(&self.wifi_radio);
        let lora_radio = Arc::clone(&self.lora_radio);
        // let logger = self.logger.clone();
        let link = match r_type {
            RadioTypes::ShortRange => String::from("wifi"),
            RadioTypes::LongRange => String::from("lora"),
        };
        LoraWifiBeacon::handle_message_internal(
            hdr, msg, link, self_peer, msg_id, wifi_radio, lora_radio, &self.logger,
        )
    }

    /// Function to initialize the protocol.
    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError> {
        //Random startup delay to avoid high collisions
        let mut rng = thread_rng();
        let startup_delay: u64 = rng.gen_range(0, 50);
        std::thread::sleep(Duration::from_millis(startup_delay));

        let wifi_radio = Arc::clone(&self.wifi_radio);
        let self_peer = self.get_self_peer();
        let logger = self.logger.clone();
        let link = String::from("wifi");
        let _wifi_beacon_handle = thread::spawn(move || {
            let _ = LoraWifiBeacon::beacon_loop(
                wifi_radio,
                WIFI_BEACON_TIMEOUT,
                self_peer,
                link,
                logger,
            );
        });

        let lora_radio = Arc::clone(&self.lora_radio);
        let self_peer = self.get_self_peer();
        let logger = self.logger.clone();
        let link = String::from("lora");
        let _lora_beacon_handle = thread::spawn(move || {
            let initial_offset: u64 = thread_rng().next_u64() % 3_000u64;
            thread::sleep(Duration::from_millis(initial_offset));
            let _ = LoraWifiBeacon::beacon_loop(
                lora_radio,
                LORA_BEACON_TIMEOUT,
                self_peer,
                link,
                logger,
            );
        });
        Ok(None)
    }

    /// Function to send command to another node in the network
    fn send(&self, _destination: String, _data: Vec<u8>) -> Result<(), MeshSimError> {
        unimplemented!("LoraWifiBeacon does not support send commands");
    }
}

impl LoraWifiBeacon {
    ///Creates a new instance of the LoraWifi protocol
    pub fn new(
        worker_name: String,
        worker_id: String,
        wifi_radio: Arc<dyn Radio>,
        lora_radio: Arc<dyn Radio>,
        rng : StdRng,
        logger: Logger,
    ) -> LoraWifiBeacon {
        LoraWifiBeacon {
            worker_name,
            worker_id,
            wifi_radio,
            lora_radio,
            rng : rng,
            logger,
        }
    }

    fn beacon_loop(
        radio: Arc<dyn Radio>,
        timeout: u64,
        self_peer: Peer,
        link: String,
        logger: Logger,
    ) -> Result<(), MeshSimError> {
        let mut counter: u64 = 0;
        let sleep_time = Duration::from_millis(timeout);

        loop {
            thread::sleep(sleep_time);
            counter += 1;
            let msg = Messages::Beacon(counter);
            let mut hdr = MessageHeader::new();
            hdr.sender = self_peer.clone();
            hdr.payload = Some(to_vec(&msg).map_err(|e| {
                let err_msg = String::from("Error serializing payload");
                MeshSimError {
                    kind: MeshSimErrorKind::Serialization(err_msg),
                    cause: Some(Box::new(e)),
                }
            })?);

            radio.broadcast(hdr)?;
            info!(logger, "Beacon sent over {}:{}", &link, counter);
        }
    }

    fn get_self_peer(&self) -> Peer {
        Peer {
            name: self.worker_name.clone(),
            id: self.worker_id.clone(),
            short_address: None,
            long_address: None,
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

    #[allow(unused)]
    fn serialize_message(msg: MessageHeader) -> Result<Vec<u8>, MeshSimError> {
        to_vec(&msg).map_err(|e| {
            let err_msg = String::from("Error serializing message");
            MeshSimError {
                kind: MeshSimErrorKind::Serialization(err_msg),
                cause: Some(Box::new(e)),
            }
        })
    }

    fn handle_message_internal(
        hdr: MessageHeader,
        msg: Messages,
        link: String,
        self_peer: Peer,
        _msg_hash: String,
        _wifi_radio: Arc<dyn Radio>,
        _lora_radio: Arc<dyn Radio>,
        logger: &Logger,
    ) -> Result<Outcome, MeshSimError> {
        match msg {
            Messages::Beacon(counter) => {
                LoraWifiBeacon::process_beacon_msg(hdr, counter, link, self_peer, logger)
            }
            Messages::BeaconResponse(counter) => {
                LoraWifiBeacon::process_beacon_response_msg(hdr, counter, link, self_peer, logger)
            }
        }
    }

    fn process_beacon_msg(
        mut hdr: MessageHeader,
        counter: u64,
        link: String,
        me : Peer,
        logger: &Logger,
    ) -> Result<Outcome, MeshSimError> {
        info!(
            logger,
            "Beacon received over {} from {}:{}", link, hdr.sender.name, counter
        );
        hdr.destination = hdr.sender.clone();
        hdr.sender = me.clone();
        hdr.payload = Some(serialize_message(Messages::BeaconResponse(counter))?);

        Ok((Some(hdr), None))
    }

    fn process_beacon_response_msg(
        hdr: MessageHeader,
        counter: u64,
        link: String,
        me: Peer,
        logger: &Logger,
    ) -> Result<Outcome, MeshSimError> {

        if hdr.destination.name == me.name {
            info!(
                logger,
                "BeaconResponse received over {} from {}:{}", link, hdr.sender.name, counter
            );
        }

        Ok((None, None))
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