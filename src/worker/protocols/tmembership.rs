//! Toy Membership protocol used to develop the platform.

extern crate serde_cbor;
extern crate rand;

use worker::protocols::Protocol;
use worker::{WorkerError, Peer, MessageHeader, Radio, Worker};
use std::collections::HashSet;
use std::sync::{Mutex, Arc, MutexGuard};
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use self::rand::{StdRng, Rng};

const HEARTBEAT_TIMER : u64 = 5000; //5000 ms

///The main struct for this protocol. Implements the worker::protocol::Protocol trait.
#[derive(Debug)]
pub struct TMembership {
    neighbours : Arc<Mutex<HashSet<Peer>>>,
    network_members : Arc<Mutex<HashSet<Peer>>>,
    short_radio : Arc<Radio>,
    rng : StdRng,
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
pub struct HeartbeatMessage;

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
        let nl = Arc::clone(&self.neighbours);
        let gl = Arc::clone(&self.network_members);
        TMembership::handle_message_internal(header, msg, self.short_radio.get_self_peer().clone(), nl, gl)
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

        // Start thread for heartbeat messages.
        let mut rng = self.rng.clone();
        let sender = self.short_radio.get_self_peer().clone();
        let short_radio = Arc::clone(&self.short_radio);
        let neighbours = Arc::clone(&self.neighbours);
        let network_members = Arc::clone(&self.network_members);

        let _handle = thread::spawn(move || -> Result<MessageHeader, WorkerError> {
            let sleep_duration = Duration::from_millis(HEARTBEAT_TIMER);

            loop {
                thread::sleep(sleep_duration);
                let nl = Arc::clone(&neighbours);


                let mut selected_peer = Peer::new();
                {   //LOCK : GET : NEAR
                    let near : MutexGuard<HashSet<Peer>> = try!(nl.lock());
                    //Select random peer
                    let p_index : u32 = rng.next_u32() % near.len() as u32;

                    let mut i = 0;
                    for dest in near.iter() {
                        if i == p_index{
                            selected_peer = dest.clone()
                        }
                        i += 1;
                    }

                    if i == 0 {
                        //Empty neighbor list
                        continue;
                    }
                }   //LOCK : RELEASE : NEAR

                let msg = try!(TMembership::create_heartbeat_message(sender.clone(), selected_peer.clone())); //Initial message of the heartbeat handshake.
                let mut response = Some(msg); 
                let mut client =  match short_radio.connect(&selected_peer) {
                    Ok(c) => { c },
                    Err(e) => { 
                        //Unable to connect to peer. Remove it from neighbor list.
                        //TODO: Actually remove it and log the action.
                        continue;
                    },
                };

                loop {
                    let gl = Arc::clone(&network_members);
                    let nl = Arc::clone(&neighbours);

                    match response {
                        Some(resp_msg) => { 
                            let _res = try!(client.send_msg(resp_msg));
                        },
                        None => {
                            //End of protocol sequence.
                            break;
                        }
                    }

                    let hdr = try!(client.read_msg());
                    response = match hdr {
                        Some(mut h) => {
                                let data = match h.payload.take() {
                                    Some(d) => { d },
                                    None => {
                                        warn!("Messaged received from {:?} had empty payload.", &h.sender);
                                        vec![]
                                    }
                                };
            
                                let msg = try!(TMembership::build_protocol_message(data)); 
                                try!(TMembership::handle_message_internal(h, msg, sender.clone(), nl, gl)) 
                        },
                        None => None,
                    };
                }

                //Print the membership lists at the end of the cycle
                let _ = TMembership::print_membership_list("Neighbors", Arc::clone(&neighbours));
                let _ = TMembership::print_membership_list("Membership", Arc::clone(&network_members));
            }
        });
        Ok(None)
    }

}

impl TMembership {
    ///Get a new empty TMembership object.
    pub fn new(sr : Arc<Radio>, seed : u32) -> TMembership {
        let n = Arc::new(Mutex::new(HashSet::new()));
        let m = Arc::new(Mutex::new(HashSet::new()));
        let rng = Worker::rng_from_seed(seed);

        TMembership{ neighbours : n, 
                     network_members : m,
                     short_radio : sr,
                     rng : rng }
    }

    fn handle_message_internal(hdr : MessageHeader, msg : Messages, me : Peer,
                               neighbours : Arc<Mutex<HashSet<Peer>>>,
                               network_members : Arc<Mutex<HashSet<Peer>>>,) -> Result<Option<MessageHeader>, WorkerError> {
        let response = match msg {
                            Messages::Join(m) => {
                                TMembership::process_join_message(hdr, m, me, neighbours, network_members)
                            },
                            Messages::Ack(m) => {
                                TMembership::process_ack_message(hdr, m, neighbours, network_members)
                            },
                            Messages::Heartbeat(m) => {
                                TMembership::process_heartbeat_message(hdr, m, neighbours, network_members)
                            },
                            Messages::Alive(m) => {
                                TMembership::process_alive_message(hdr, m, neighbours, network_members)
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
    fn process_join_message(hdr : MessageHeader, _msg : JoinMessage, me : Peer,
                            neighbours : Arc<Mutex<HashSet<Peer>>>,
                            network_members : Arc<Mutex<HashSet<Peer>>>) -> Result<Option<MessageHeader>, WorkerError> {
        info!("Received JOIN message from {}", hdr.sender.name);

        {
            //LOCK : GET : NEAR
            let mut near = try!(neighbours.lock());
            
            //Presumably, the sender is a new node joining. If already in our neighbour list, this is a NOP.
            let _res = near.insert(hdr.sender.clone());
        }   //LOCK : RELEASE : NEAR

        {
            //LOCK : GET : GNL
            let mut gnl = try!(network_members.lock());

            //Build the internal message of the response
            let msg_data = AckMessage{ global_peer_list : gnl.clone()};

            //Add the sender to our own GNL.
            gnl.insert(hdr.sender.clone());

            //Build the ACK message for the sender
            let ack_msg = Messages::Ack(msg_data);
            let payload = try!(to_vec(&ack_msg));
            info!("Built ACK message for sender: {}, address {}", &hdr.sender.name, &hdr.sender.address);
            
            //Build the message header that's ready for sending.
            let response = MessageHeader{   sender : me, 
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
    fn process_ack_message( hdr : MessageHeader, msg : AckMessage,
                            neighbours : Arc<Mutex<HashSet<Peer>>>,
                            network_members : Arc<Mutex<HashSet<Peer>>>) -> Result<Option<MessageHeader>, WorkerError> {        
        info!("Received ACK message from {}", hdr.sender.name);
        {
            //LOCK : GET : NEAR
            let mut near = try!(neighbours.lock());
            
            let _res = near.insert(hdr.sender.clone());
        }   //LOCK : RELEASE : NEAR

        {
            //LOCK : GET : GNL
            let mut gnl = try!(network_members.lock());

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

    /// A message that is sent periodically to a given amount of members
    /// of the nearby list to check if they are still alive.
    /// When to send: After the heartbeat timer expires.
    /// Receiver: Random member of the nearby peer list.
    /// Payload: global node list.
    /// Actions: 
    ///   1. Add the sent peers to the suspected list.
    fn create_heartbeat_message(sender : Peer, destination : Peer) -> Result<MessageHeader, WorkerError> {
        let data = HeartbeatMessage;
        let hb_msg = Messages::Heartbeat(data);
        let payload = try!(to_vec(&hb_msg));
        info!("Built HEARTBEAT message for peer: {}, address {}", &destination.name, &destination.address);
        
        //Build the message header that's ready for sending.
        let msg = MessageHeader{ sender : sender, 
                                 destination : destination, 
                                 payload : Some(payload) };
        Ok(msg)
    }

    /// A response to a heartbeat message.
    /// When to send: After receiving a heartbeat message.
    /// Sender: A nearby peer in range.
    /// Payload: global node list.
    /// Actions:
    ///   1. Construct an alive message with this peer's GNL and send it.
    ///   2. Add the sender to the neighbor list (if not there already)
    fn process_heartbeat_message(hdr : MessageHeader, msg : HeartbeatMessage,
                                 neighbours : Arc<Mutex<HashSet<Peer>>>,
                                 network_members : Arc<Mutex<HashSet<Peer>>>) -> Result<Option<MessageHeader>, WorkerError> {
        info!("Received Heartbeat message from {}, address {}", hdr.sender.name, hdr.sender.address);
        
        {
            //LOCK : GET : NL
            let mut nl = try!(neighbours.lock());

            //If the sender peer is not in our current neihbor list, add it.
            if nl.contains(&hdr.sender) {
                nl.insert(hdr.sender.clone());
            }
        } //LOCK : RELEASE : NL

        //Obtain a copy of our current GNL that's necesary for the response message.
        let gnl_cache : HashSet<Peer> = match network_members.lock() { //LOCK : GET : GNL
            Ok(gnl) => { gnl.clone() },
            Err(e) => {
                return Err(WorkerError::Sync(e.to_string()))
            },
        }; //LOCK : RELEASE : GNL

        //Craft response
        let data = AliveMessage{ global_peer_list : gnl_cache };
        //debug!("Message payload: {:?}", &data);
        let alive_msg = Messages::Alive(data);
        let payload = try!(to_vec(&alive_msg));
        info!("Built Alive message for peer: {}, address {}", &hdr.sender.name, &hdr.sender.address);
        
        //Build the message header that's ready for sending.
        let msg = MessageHeader{ sender : hdr.destination, 
                                 destination : hdr.sender, 
                                 payload : Some(payload) };
        //debug!("Message: {:?}", &msg);
        Ok(Some(msg))
    }

    /// The final part of the heartbeat-alive part of the protocol. If this message is 
    /// received, it means a peer to which we sent a heartbeat message is still alive.
    /// Sender: A nearby peer in range that received a heartbeat message from this peer.
    /// Payload: global node list.
    /// Actions:
    ///   1. Add the difference between the senders GNL and this GNL.
    ///   2. Remove the sender from the list of suspected dead peers.
    fn process_alive_message(hdr : MessageHeader, msg : AliveMessage,
                             _neighbours : Arc<Mutex<HashSet<Peer>>>,
                             network_members : Arc<Mutex<HashSet<Peer>>>) -> Result<Option<MessageHeader>, WorkerError> {
        info!("Received Alive message from {}.", hdr.sender.name);

        {
            //LOCK : GET : GNL
            let mut gnl = try!(network_members.lock());

            //Get the peers (if any) in the senders GNL that are not in the current node's list.
            let gnl_cache = gnl.clone();
            let diff = msg.global_peer_list.difference(&gnl_cache);
            //Add those elements to the current GNL
            let _res = diff.map(|x| { 
                            info!("Adding peer {}/{} to list.", x.name, x.id);            
                            gnl.insert(x.clone());
                       });
        } //LOCK : RELEASE : GNL

        Ok(None)
    }

    fn print_membership_list<'a>(name : &'a str, list : Arc<Mutex<HashSet<Peer>>>) -> Result<(), WorkerError> {
        let l = try!(list.lock());

        info!("Members of list {}", name);
        for m in l.iter() {
            info!("{} : {}", m.name, m.id);
        }
        Ok(())
    }
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