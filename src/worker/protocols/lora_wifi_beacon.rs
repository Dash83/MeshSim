//! Module for the test protocol that uses Lora and Wifi radios

use crate::worker::protocols::{Transmission, HandleMessageOutcome, Protocol, ProtocolMessages};
use crate::worker::radio::{self, Radio, RadioTypes};
use crate::worker::{MessageHeader, MessageStatus};
use crate::{MeshSimError, MeshSimErrorKind};

use chrono::{DateTime, Utc};
use rand::thread_rng;
use rand::{rngs::StdRng, Rng};
use serde_cbor::de::*;
use serde_cbor::ser::*;
use slog::{Logger, Record, Serializer, KV};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use crossbeam_channel::Sender;

const WIFI_BEACON_TIMEOUT: u64 = 2_000;
const LORA_BEACON_TIMEOUT: u64 = 2_000;

///Base structure to hold the state of the LoraWifiBeacon protocol
#[derive(Debug)]
pub struct LoraWifiBeacon {
    worker_name: String,
    worker_id: String,
    wifi_tx_queue: Sender<Transmission>,
    lora_tx_queue: Sender<Transmission>,
    rng: StdRng,
    logger: Logger,
}
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct BeaconMessage(pub u64);

///Messages used in this protocol
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Messages {
    Beacon(BeaconMessage),
    BeaconResponse(BeaconMessage),
}

impl KV for Messages {
    fn serialize(&self, _rec: &Record, serializer: &mut dyn Serializer) -> slog::Result {
        match *self {
            Messages::Beacon(msg) => {
                let _ = serializer.emit_str("msg_type", "BEACON")?;
                serializer.emit_u64("id", msg.0)
            }
            Messages::BeaconResponse(msg) => {
                let _ = serializer.emit_str("msg_type", "BEACON_RESPONSE")?;
                serializer.emit_u64("id", msg.0)
            }
        }
    }
}

impl Protocol for LoraWifiBeacon {
    fn handle_message(
        &self,
        hdr: MessageHeader,
        _ts: DateTime<Utc>,
        r_type: RadioTypes,
    ) -> Result<(), MeshSimError> {
        //Filter out packets coming from this node, as we get many from the multicast.
        if hdr.sender == self.worker_name {
            return Ok(());
        }

        let msg = deserialize_message(hdr.get_payload())?;
        let self_peer = self.get_self_peer();

        // let logger = self.logger.clone();
        let link = match r_type {
            RadioTypes::ShortRange => String::from("wifi"),
            RadioTypes::LongRange => String::from("lora"),
        };
        let resp = LoraWifiBeacon::handle_message_internal(
            hdr,
            msg,
            link,
            self_peer,
            &self.logger,
        )?;

        let tx_queue = match r_type {
            RadioTypes::ShortRange => self.wifi_tx_queue.clone(),
            RadioTypes::LongRange => self.lora_tx_queue.clone(),
        };
        if let Some((resp_hdr, md)) = resp {
            tx_queue.send((resp_hdr, md, Utc::now()))
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

    /// Function to initialize the protocol.
    fn init_protocol(&self) -> Result<Option<MessageHeader>, MeshSimError> {
        //Random startup delay to avoid high collisions
        let mut rng = thread_rng();
        let startup_delay: u64 = rng.gen_range(0, 50);
        std::thread::sleep(Duration::from_millis(startup_delay));

        let wifi_tx_queue = self.wifi_tx_queue.clone();
        let self_peer = self.get_self_peer();
        let logger = self.logger.clone();
        let link = String::from("wifi");
        let _wifi_beacon_handle = thread::spawn(move || {
            let _ = LoraWifiBeacon::beacon_loop(
                wifi_tx_queue,
                WIFI_BEACON_TIMEOUT,
                self_peer,
                link,
                logger,
            );
        });

        let lora_tx_queue = self.lora_tx_queue.clone();
        let self_peer = self.get_self_peer();
        let logger = self.logger.clone();
        let link = String::from("lora");
        let _lora_beacon_handle = thread::spawn(move || {
            // let initial_offset: u64 = thread_rng().next_u64() % 3_000u64;
            // thread::sleep(Duration::from_millis(initial_offset));
            let _ = LoraWifiBeacon::beacon_loop(
                lora_tx_queue,
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

    fn do_maintenance(&self) -> Result<(), MeshSimError> {
        // Nothing to do
        Ok(())
    }
}

impl LoraWifiBeacon {
    ///Creates a new instance of the LoraWifi protocol
    pub fn new(
        worker_name: String,
        worker_id: String,
        wifi_tx_queue: Sender<Transmission>,
        lora_tx_queue: Sender<Transmission>,
        rng: StdRng,
        logger: Logger,
    ) -> LoraWifiBeacon {
        LoraWifiBeacon {
            worker_name,
            worker_id,
            wifi_tx_queue,
            lora_tx_queue,
            rng,
            logger,
        }
    }

    fn beacon_loop(
        tx_queue: Sender<Transmission>,
        timeout: u64,
        self_peer: String,
        _link: String,
        logger: Logger,
    ) -> Result<(), MeshSimError> {
        let mut counter: u64 = 0;
        let sleep_time = Duration::from_millis(timeout);

        loop {
            thread::sleep(sleep_time);
            counter += 1;
            let msg = Messages::Beacon(BeaconMessage(counter));
            let log_data = ProtocolMessages::LoraWifi(msg.clone());
            let hdr = MessageHeader::new(self_peer.clone(), String::new(), serialize_message(msg)?);

            tx_queue.send((hdr, log_data, Utc::now()))
            .map_err(|e| { 
                let msg = format!("Failed to queue response into tx_queue");
                MeshSimError {
                    kind: MeshSimErrorKind::Contention(msg),
                    cause: Some(Box::new(e)),
                }
            })?;
        }
    }

    fn get_self_peer(&self) -> String {
        self.worker_name.clone()
    }

    fn handle_message_internal(
        hdr: MessageHeader,
        msg: Messages,
        link: String,
        self_peer: String,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        match msg {
            Messages::Beacon(msg) => {
                LoraWifiBeacon::process_beacon_msg(hdr, msg, link, self_peer, logger)
            }
            Messages::BeaconResponse(msg) => {
                LoraWifiBeacon::process_beacon_response_msg(hdr, msg, link, self_peer, logger)
            }
        }
    }

    fn process_beacon_msg(
        hdr: MessageHeader,
        msg: BeaconMessage,
        _link: String,
        me: String,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        radio::log_handle_message(
            logger,
            &hdr,
            MessageStatus::ACCEPTED,
            None,
            Some("Replying to message"),
            &Messages::Beacon(msg.clone()),
        );

        let sender = hdr.sender;
        let resp_msg = Messages::BeaconResponse(msg);
        let log_data = ProtocolMessages::LoraWifi(resp_msg.clone());
        let response_hdr = MessageHeader::new(me, sender, serialize_message(resp_msg)?);
        Ok(Some((response_hdr, log_data)))
    }

    fn process_beacon_response_msg(
        hdr: MessageHeader,
        msg: BeaconMessage,
        _link: String,
        me: String,
        logger: &Logger,
    ) -> Result<HandleMessageOutcome, MeshSimError> {
        if hdr.destination == me {
            // info!(
            //     logger,
            //     "BeaconResponse received over {} from {}:{}", link, hdr.sender, counter
            // );
            radio::log_handle_message(
                logger,
                &hdr,
                MessageStatus::ACCEPTED,
                None,
                None,
                &Messages::BeaconResponse(msg),
            );
        }

        Ok(None)
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
