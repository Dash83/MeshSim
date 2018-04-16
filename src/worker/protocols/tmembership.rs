//! Toy Membership protocol used to develop the platform.

extern crate serde_cbor;

use worker::protocols::Protocol;
use worker::{WorkerError, Peer, MessageHeader};
use std::collections::HashSet;
use self::serde_cbor::de::*;
//use self::serde_cbor::ser::*;

///The main struct for this protocol. Implements the worker::protocol::Protocol trait.
pub struct TMembership;

/// This enum represents the types of network messages supported in the protocol as well as the
/// data associated with them. For each message type, an associated struct will be created to represent 
/// all the data needed to operate on such message.
#[derive(Debug, Serialize, Deserialize)]
pub enum MessageType {
    ///Message that a peer sends to join the network.
    Join(JoinMessage),
    ///Reply to a JOIN message sent from a current member of the network.
    Ack(AckMessage),
    ///Hearbeat message to check on the health of a peer.
    Heartbeat(HeartbeatMessage),
    ///Alive message, which is a response to the heartbeat message.
    Alive(AliveMessage),
}

/// The type of message passed as payload for Join messages.
/// The actual message is not required at this point, but used for future compatibility.
#[derive(Debug, Serialize, Deserialize)]
pub struct JoinMessage;

/// Ack message used to reply to Join messages
#[derive(Debug, Serialize, Deserialize)]
pub struct AckMessage {
    global_peer_list : HashSet<Peer>,
}

/// General purposes data message for the network
#[derive(Debug, Serialize, Deserialize)]
pub struct DataMessage;

/// Heartbeat message used to check on the status of nearby peers.
#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    global_peer_list : HashSet<Peer>,
}

/// Response message to the heartbeat message.
#[derive(Debug, Serialize, Deserialize)]
pub struct AliveMessage {
    global_peer_list : HashSet<Peer>,
}

impl Protocol for TMembership {
    fn handle_message(&self, encapsulated_message : MessageHeader) -> Option<MessageHeader> {
        None
    }

    // fn build_message(&self, data : Vec<u8>) -> Result<MessageHeader, WorkerError> {
    //     let msg : Result<MessageType, _> = try!(from_reader(&data[..]));
        
    //     Ok(msg_type)
    // }
}

impl TMembership {
    ///Get a new empty TMembership object.
    pub fn new() -> TMembership {
        TMembership{}
    }

    fn handle_message_internal(&mut self, encapsulated_message : MessageType) -> Result<(), WorkerError> {
        match encapsulated_message {
            MessageType::Join(msg) => {
                //self.process_join_message(msg)
            },
            MessageType::Ack(msg) => {
                //self.process_ack_message(msg)
            },
            MessageType::Heartbeat(msg) => {
                //self.process_heartbeat_message(msg)
            },
            MessageType::Alive(msg) => {
                //self.process_alive_message(msg)
            }
        }
        Ok(())
    }
/*
    /// The first message of the protocol. 
    /// When to send: At start-up, after doing a scan of nearby peers.
    /// Receiver: All Nearby peers.
    /// Payload: Self Peer object.
    /// Actions: None.
    fn send_join_message(&self) {
        for r in &self.radios {
            //Send messge to each Peer reachable by this radio
            for p in &self.nearby_peers {
                info!("Sending join message to {}, address {}", p.name, p.public_key);
                let data = JoinMessage { sender : self.me.clone()};
                let msg = MessageType::Join(data);
                let _ = r.send(msg, p);
            }

        }
    }

    /// A response to a new peer joining the network.
    /// When to send: After receiving a join message.
    /// Sender: A new peer in range joining the network.
    /// Payload: Global peer list.
    /// Actions:
    ///   1. Add sender to global membership list and nearby peer list.
    ///   2. Construct an ACK message and reply with it.
    fn process_join_message(&mut self, msg : JoinMessage) -> Result<(), WorkerError> {
        info!("Received JOIN message from {}", msg.sender.name);
        //Add new node to nearby peer list
        self.nearby_peers.insert(msg.sender.clone());
        //Obtain a reference to our current GPL.
        let gpl = self.global_peer_list.clone();
        //Obtain a lock to the underlying data.
        let mut gpl = gpl.lock().unwrap();
        gpl.insert(msg.sender.clone());

        //Respond with ACK 
        //Need to responde through the same radio we used to receive this.
        // For now just use default radio.
        let data = AckMessage{sender: self.me.clone(), global_peer_list : self.nearby_peers.clone()};
        let ack_msg = MessageType::Ack(data);
        info!("Sending ACK message to {}, address {}", msg.sender.name, msg.sender.address);
        /*
        match self.radios[0].send(ack_msg, &msg.sender) {
            Ok(_) => info!("ACK message sent"),
            Err(e) => error!("ACK message failed to be sent. Error:{}", e),
        };*/
        self.radios[0].send(ack_msg, &msg.sender)
    }

    /// The final part of the protocol's initial handshake
    /// When to send: After receiving an ACK message.
    /// Sender: A peer in the network that received a JOIN message.
    /// Payload: Global peer list.
    /// Actions:
    ///   1. Add the difference between the payload and current GPL to the GPL.
    fn process_ack_message(&mut self, msg: AckMessage) -> Result<(), WorkerError> {
        info!("Received ACK message from {}", msg.sender.name);
        //Obtain a reference to our current GPL.
        let gpl = self.global_peer_list.clone();
        //Obtain a lock to the underlying data.
        let mut gpl = gpl.lock().unwrap();

        //This worker just started, but might be getting an ACK message from many
        //nearby peers. Diff the incoming set with the current set using an outer join.
        let gpl_snapshot = gpl.clone();
        for p in msg.global_peer_list.difference(&gpl_snapshot) {
            info!("Adding peer {}/{} to list.", p.name, p.public_key);
            gpl.insert(p.clone());
        }
        Ok(())
    }

    /// A message that is sent periodically to a given amount of members
    /// of the nearby list to check if they are still alive.
    /// When to send: After the heartbeat timer expires.
    /// Receiver: Random member of the nearby peer list.
    /// Payload: Global peer list.
    /// Actions: 
    ///   1. Add the sent peers to the suspected list.
    fn send_heartbeat_message(&self) {
        //Get the RNG
        let mut random_bytes = vec![];
        let _ = random_bytes.write_u32::<NativeEndian>(self.random_seed);
        let randomness : Vec<usize> = random_bytes.iter().map(|v| *v as usize).collect();
        let mut gen = StdRng::from_seed(randomness.as_slice());

        //Will contact a given amount of current number of nearby peers.
        let num_of_peers = self.nearby_peers.len() as f32 * GOSSIP_FACTOR;
        let num_of_peers = num_of_peers.ceil() as u32;

        //Get a handle and lock on the suspected peer list.
        let spl = self.suspected_list.clone();
        let mut spl = spl.lock().unwrap();

        //Get a handle and lock of the GPL
        let gpl = self.global_peer_list.clone();
        let gpl = gpl.lock().unwrap();

        //Get a copy of the GPL for iteration and selecting the random peers.
        let gpl_iter = gpl.clone();
        let mut gpl_iter = gpl_iter.iter();
        for _i in 0..num_of_peers {
            let selection : usize = gen.next_u32() as usize % self.nearby_peers.len();
            let mut j = 0;
            while j < selection {
                gpl_iter.next();
                j += 1;
            }
            //The chosen peer
            let recipient = gpl_iter.next().unwrap();

            //Add the suspected peer to the suspected list.
            spl.push(recipient.clone());

            let gpl_snapshot = gpl.clone();
            let data = HeartbeatMessage{ sender : self.me.clone(),
                                        global_peer_list : gpl_snapshot};
            let msg = MessageType::Heartbeat(data);
            info!("Sending a Heartbeat message to {}, address {}", recipient.name, recipient.address);
            let _res = self.radios[0].send(msg, recipient);
        }
    }

    /// A response to a heartbeat message.
    /// When to send: After receiving a heartbeat message.
    /// Sender: A nearby peer in range.
    /// Payload: Global peer list.
    /// Actions:
    ///   1. Construct an alive message with this peer's GPL and send it.
    ///   2. Add the difference between the senders GPL and this GPL.
    fn process_heartbeat_message(&self, msg : HeartbeatMessage) -> Result<(), WorkerError> {
        info!("Received Heartbeat message from {}, address {}", msg.sender.name, msg.sender.address);
        //The sender is asking if I'm alive. Before responding, let's take a look at their GPL
        //Obtain a reference to our current GPL.
        let gpl = self.global_peer_list.clone();
        //Obtain a lock to the underlying data.
        let mut gpl = gpl.lock().unwrap();

        //Diff the incoming set with the current set using an outer join.
        let gpl_snapshot = gpl.clone();
        for p in msg.global_peer_list.difference(&gpl_snapshot) {
            info!("Adding peer {}/{} to list.", p.name, p.public_key);
            gpl.insert(p.clone());
        }

        //Now construct and send an alive message to prove we are alive.
        let data = AliveMessage{ sender : self.me.clone(),
                                 global_peer_list : gpl.clone()};
        let alive_msg = MessageType::Alive(data);
        self.radios[0].send(alive_msg, &msg.sender)
    }

    /// The final part of the heartbeat-alive part of the protocol. If this message is 
    /// received, it means a peer to which we sent a heartbeat message is still alive.
    /// Sender: A nearby peer in range that received a heartbeat message from this peer.
    /// Payload: Global peer list.
    /// Actions:
    ///   1. Add the difference between the senders GPL and this GPL.
    ///   2. Remove the sender from the list of suspected dead peers.
    fn process_alive_message(&self, msg : AliveMessage) -> Result<(), WorkerError> {
        info!("Received Alive message from {}, address {}", msg.sender.name, msg.sender.address);
        //Get a handle and lock on the suspected peer list.
        let spl = self.suspected_list.clone();
        let mut spl = spl.lock().unwrap();
        //Find the index of the element to remove.
        let mut index : i32 = -1;
        let mut j = 0;
        let spl_iter = spl.clone();
        let spl_iter = spl_iter.iter();
        for p in spl_iter {
            if *p == msg.sender {
                index = j;
                break; //Found the element
            }
            j += 1;
        }
        //Remove the sender from the suspected list.
        if index > 0 {
            let index = index as usize;
            spl.remove(index);
        } else {
            //Received an alive message from a peer that was not in the list.
            //In that case, ignore and return.
            warn!("Received ALIVE message from {}, address {}, that was not in the spl", msg.sender.name, msg.sender.address);
            return Ok(())
        }

        //Obtain a reference to our current GPL.
        let gpl = self.global_peer_list.clone();
        //Obtain a lock to the underlying data.
        let mut gpl = gpl.lock().unwrap();

        //Diff the incoming set with the current set using an outer join.
        let gpl_snapshot = gpl.clone();
        for p in msg.global_peer_list.difference(&gpl_snapshot) {
            info!("Adding peer {}/{} to list.", p.name, p.public_key);
            gpl.insert(p.clone());
        }
        Ok(())
    }
*/
}