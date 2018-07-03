//! Toy Membership protocol used to develop the platform.

extern crate serde_cbor;

use worker::protocols::Protocol;
use worker::{WorkerError, Peer, MessageHeader, Radio};
use std::collections::HashSet;
use std::sync::{Mutex, Arc};
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;

///The main struct for this protocol. Implements the worker::protocol::Protocol trait.
#[derive(Debug)]
pub struct TMembership {
    neighbours : Arc<Mutex<HashSet<Peer>>>,
    network_members : Arc<Mutex<HashSet<Peer>>>,
    short_radio : Box<Radio>,
}

/// This enum represents the types of network messages supported in the protocol as well as the
/// data associated with them. For each message type, an associated struct will be created to represent 
/// all the data needed to operate on such message.
#[derive(Debug, Serialize, Deserialize)]
pub enum Messages {
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
    fn handle_message(&self, mut header : MessageHeader) -> Result<Option<MessageHeader>, WorkerError> {
        let data = match header.payload.take() {
            Some(d) => { d },
            None => {
                warn!("Messaged received from {:?} had empty payload.", header.sender);
                return Ok(None)
            }
        };
        
        let msg = try!(TMembership::build_protocol_message(data));
        self.handle_message_internal(header, msg)
    }

    fn init_protocol(&self) -> Result<Option<MessageHeader>, WorkerError>{
        //Update peers with initial scan data.
        let nearby_peers = try!(self.short_radio.scan_for_peers());

        //Send join messages for all of said peers
        for p in nearby_peers {
            //Create join message
            let msg = try!(self.create_join_message(p));
            //Connect to the remote peer
            let mut c = try!(self.short_radio.connect(&msg.destination));
            //Send initial message
            let _res = try!(c.send_msg(msg));

            //We will now enter a read/response loop until the protocol finishes.
            loop {
                //Read the data from the unix socket
                let recv_msg = try!(c.read_msg());
                //Try to decode the data into a message.
                let response = match recv_msg {
                    Some(m) => { try!(self.handle_message(m)) },
                    None => None,
                };
                //let response = try!(self.handle_message(recv_msg));
                match response {
                    Some(resp_msg) => { 
                        let _res = try!(c.send_msg(resp_msg));
                    },
                    None => {
                        //End of protocol sequence.
                        break;
                    }
                }
            }            
            //info!("Sending join message to {}, address {}", p.name, p.public_key);
        }

        //TODO: Start thread for heartbeat messages.

        Ok(None)
    }

}

impl TMembership {
    ///Get a new empty TMembership object.
    pub fn new(sr : Box<Radio>) -> TMembership {
        let n = Arc::new(Mutex::new(HashSet::new()));
        let m = Arc::new(Mutex::new(HashSet::new()));

        TMembership{ neighbours : n, 
                     network_members : m,
                     short_radio : sr }
    }

    fn handle_message_internal(&self, hdr : MessageHeader, msg : Messages) -> Result<Option<MessageHeader>, WorkerError> {
        let response = match msg {
                            Messages::Join(m) => {
                                self.process_join_message(hdr, m)
                            },
                            Messages::Ack(msg) => {
                                self.process_ack_message(hdr, msg)
                            },
                            Messages::Heartbeat(_msg) => {
                                //self.process_heartbeat_message(msg)
                                Ok(None)
                            },
                            Messages::Alive(_msg) => {
                                //self.process_alive_message(msg)
                                Ok(None)
                            }
                       };
        response
    }

    fn build_protocol_message(data : Vec<u8>) -> Result<Messages, serde_cbor::Error> {
        let res : Result<Messages, serde_cbor::Error> = from_slice(data.as_slice());
        res
    }


    /// The first message of the protocol. 
    /// When to send: At start-up, after doing a scan of nearby peers.
    /// Receiver: All Nearby peers.
    /// Payload: Self Peer object.
    /// Actions: None.
    fn create_join_message(&self, destination : Peer) -> Result<MessageHeader, WorkerError> {
        let data = JoinMessage;
        let join_msg = Messages::Join(data);
        let payload = try!(to_vec(&join_msg));
        info!("Built JOIN message for peer: {}, address {}", &destination.name, &destination.address);
        
        //Build the message header that's ready for sending.
        let msg = MessageHeader{ sender : self.short_radio.get_self_peer().clone(), 
                                 destination : destination, 
                                 payload : Some(payload) };
        Ok(msg)
    }

    /// A response to a new peer joining the network.
    /// When to send: After receiving a join message.
    /// Sender: A new peer in range of the current node, trying to join the network.
    /// Payload: None.
    /// Actions:
    ///   1. Add sender to global node list and nearby peer list.
    ///   2. Construct an ACK message and reply with it.
    fn process_join_message(&self, hdr : MessageHeader, _msg : JoinMessage) -> Result<Option<MessageHeader>, WorkerError> {
        info!("Received JOIN message from {}", hdr.sender.name);

        //Obtain reference to our neighbours list
        let near = Arc::clone(&self.neighbours);
        {
            //LOCK : GET : NEAR
            let mut near = try!(near.lock());
            
            //Presumably, the sender is a new node joining. If already in our neighbour list, this is a NOP.
            let _res = near.insert(hdr.sender.clone());
        }   //LOCK : RELEASE : NEAR

        //Obtain a reference to our current GNL.
        let gnl = Arc::clone(&self.network_members);
        {
            //LOCK : GET : GNL
            let mut gnl = try!(gnl.lock());

            //Build the internal message of the response
            let msg_data = AckMessage{ global_peer_list : gnl.clone()};

            //Add the sender to our own GNL.
            gnl.insert(hdr.sender.clone());

            //Build the ACK message for the sender
            let ack_msg = Messages::Ack(msg_data);
            let payload = try!(to_vec(&ack_msg));
            info!("Built ACK message for sender: {}, address {}", &hdr.sender.name, &hdr.sender.address);
            
            //Build the message header that's ready for sending.
            let response = MessageHeader{   sender : hdr.destination, 
                                        destination : hdr.sender, 
                                        payload : Some(payload) };
            Ok(Some(response))
        } //LOCK : RELEASE : GNL
    }

    /// The final part of the protocol's initial handshake
    /// When to send: After receiving an ACK message.
    /// Sender: A peer in the network that received a JOIN message.
    /// Payload: global node list.
    /// Actions:
    ///   1. Add the difference between the payload and current GNL to the GNL.
    fn process_ack_message(&self, hdr : MessageHeader, msg : AckMessage) -> Result<Option<MessageHeader>, WorkerError> {        
        info!("Received ACK message from {}", hdr.sender.name);

        //Obtain a reference to our current GNL.
        let gnl = Arc::clone(&self.network_members);
        {
            //LOCK : GET : GNL
            let mut gnl = try!(gnl.lock());

            //Get the peers (if any) in the senders GNL that are not in the current node's list.
            let gnl_cache = gnl.clone();
            let diff = msg.global_peer_list.difference(&gnl_cache);
            //Add those elements to the current GNL
            let _res = diff.map(|x| { 
                            info!("Adding peer {}/{} to list.", x.name, x.id);            
                            gnl.insert(x.clone());
                       });
        } //LOCK : RELEASE : GNL

        Ok(None) //Protocol handshake ends here.
    }
/*
    /// A message that is sent periodically to a given amount of members
    /// of the nearby list to check if they are still alive.
    /// When to send: After the heartbeat timer expires.
    /// Receiver: Random member of the nearby peer list.
    /// Payload: global node list.
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

        //Get a handle and lock of the GNL
        let gnl = self.global_peer_list.clone();
        let gnl = gnl.lock().unwrap();

        //Get a copy of the GNL for iteration and selecting the random peers.
        let gnl_iter = gnl.clone();
        let mut gnl_iter = gnl_iter.iter();
        for _i in 0..num_of_peers {
            let selection : usize = gen.next_u32() as usize % self.nearby_peers.len();
            let mut j = 0;
            while j < selection {
                gnl_iter.next();
                j += 1;
            }
            //The chosen peer
            let recipient = gnl_iter.next().unwrap();

            //Add the suspected peer to the suspected list.
            spl.push(recipient.clone());

            let gnl_snapshot = gnl.clone();
            let data = HeartbeatMessage{ sender : self.me.clone(),
                                        global_peer_list : gnl_snapshot};
            let msg = MessageType::Heartbeat(data);
            info!("Sending a Heartbeat message to {}, address {}", recipient.name, recipient.address);
            let _res = self.radios[0].send(msg, recipient);
        }
    }

    /// A response to a heartbeat message.
    /// When to send: After receiving a heartbeat message.
    /// Sender: A nearby peer in range.
    /// Payload: global node list.
    /// Actions:
    ///   1. Construct an alive message with this peer's GNL and send it.
    ///   2. Add the difference between the senders GNL and this GNL.
    fn process_heartbeat_message(&self, msg : HeartbeatMessage) -> Result<(), WorkerError> {
        info!("Received Heartbeat message from {}, address {}", msg.sender.name, msg.sender.address);
        //The sender is asking if I'm alive. Before responding, let's take a look at their GNL
        //Obtain a reference to our current GNL.
        let gnl = self.global_peer_list.clone();
        //Obtain a lock to the underlying data.
        let mut gnl = gnl.lock().unwrap();

        //Diff the incoming set with the current set using an outer join.
        let gnl_snapshot = gnl.clone();
        for p in msg.global_peer_list.difference(&gnl_snapshot) {
            info!("Adding peer {}/{} to list.", p.name, p.public_key);
            gnl.insert(p.clone());
        }

        //Now construct and send an alive message to prove we are alive.
        let data = AliveMessage{ sender : self.me.clone(),
                                 global_peer_list : gnl.clone()};
        let alive_msg = MessageType::Alive(data);
        self.radios[0].send(alive_msg, &msg.sender)
    }

    /// The final part of the heartbeat-alive part of the protocol. If this message is 
    /// received, it means a peer to which we sent a heartbeat message is still alive.
    /// Sender: A nearby peer in range that received a heartbeat message from this peer.
    /// Payload: global node list.
    /// Actions:
    ///   1. Add the difference between the senders GNL and this GNL.
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

        //Obtain a reference to our current GNL.
        let gnl = self.global_peer_list.clone();
        //Obtain a lock to the underlying data.
        let mut gnl = gnl.lock().unwrap();

        //Diff the incoming set with the current set using an outer join.
        let gnl_snapshot = gnl.clone();
        for p in msg.global_peer_list.difference(&gnl_snapshot) {
            info!("Adding peer {}/{} to list.", p.name, p.public_key);
            gnl.insert(p.clone());
        }
        Ok(())
    }
*/
}

    //Unit test for: Worker::handle_message
    //At this point, I don't know how to test this function. The function uses network connections to communciate to another process,
    //so I don't know how to mock that out in rust.
    /*
    #[test]
    fn test_worker_handle_message() {
        unimplemented!();
    }
    */

    //Unit test for: Worker::join_network
    //At this point, I don't know how to test this function. The function uses network connections to communciate to another process,
    //so I don't know how to mock that out in rust.
    /*
    #[test]
    fn test_worker_send_join_message() {
        unimplemented!();
    }
    */

    //Unit test for: Worker::process_join_message
    //At this point, I don't know how to test this function. The function uses network connections to communciate to another process,
    //so I don't know how to mock that out in rust.
    /*
    #[test]
    fn test_worker_process_join_message() {
        unimplemented!();
    }
    */