//! Toy Membership protocol used to develop the platform.

extern crate serde_cbor;
extern crate rand;

use worker::protocols::Protocol;
use worker::{WorkerError, Peer, MessageHeader, Worker, AddressType};
use worker::radio::*;
use std::collections::HashSet;
use std::sync::{Mutex, Arc, MutexGuard};
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use self::rand::{StdRng, Rng};

const HEARTBEAT_TIMER : u64 = 3000; //3000 ms

///The main struct for this protocol. Implements the worker::protocol::Protocol trait.
#[derive(Debug)]
pub struct TMembership {
    neighbours : Arc<Mutex<HashSet<Peer>>>,
    network_members : Arc<Mutex<HashSet<Peer>>>,
    worker_name : String,
    worker_id : String,
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
    fn handle_message(&self, mut header : MessageHeader, r_type : RadioTypes) -> Result<Option<MessageHeader>, WorkerError> {
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
        TMembership::handle_message_internal(header, msg, self.get_self_peer(), nl, gl)
    }

    fn init_protocol(&self) -> Result<Option<MessageHeader>, WorkerError>{
        //Perform initial scan
        let _res = self.initial_join_scan()?;

        // Start thread for heartbeat messages.
        let h = self.heartbeat_thread()?;
        
        //Create a thread to join on the threads and log any errors.
        TMembership::log_thread_errors(vec![h]);
        
        Ok(None)
    }

    fn send(&self, destination : String, data : Vec<u8>) -> Result<(), WorkerError> {
        unimplemented!()
    }
}

impl TMembership {
    ///Get a new empty TMembership object.
    pub fn new(sr : Arc<Radio>, seed : u32, id : String, name : String) -> TMembership {
        let n = Arc::new(Mutex::new(HashSet::new()));
        
        let mut members = HashSet::new();
        let me = Peer{ id : id.clone(),
                       name : name.clone(),
                       addresses : vec![AddressType::ShortRange(String::from(sr.get_address()))] };
        members.insert(me);
        let m = Arc::new(Mutex::new(members));
        let rng = Worker::rng_from_seed(seed);

        TMembership{ neighbours : n, 
                     network_members : m,
                     worker_name : name,
                     worker_id : id,
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
    fn create_join_message(sender : Peer, destination : Peer) -> Result<MessageHeader, WorkerError> {
        let join_msg = Messages::Join(JoinMessage);
        let payload = to_vec(&join_msg)?;
        info!("Built JOIN message for peer: {}, id {:?}", &destination.name, destination.id);
        
        //Build the message header that's ready for sending.
        let msg = MessageHeader{ sender : sender, 
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
            info!("Built ACK message for sender: {}, id {:?}", &hdr.destination.name, &hdr.destination.id);
            
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
            info!("Added peer {}/{} to neighbours list", &hdr.sender.name, &hdr.sender.id);
        }   //LOCK : RELEASE : NEAR

        {
            //LOCK : GET : GNL
            let mut gnl = try!(network_members.lock());

            //Get the peers (if any) in the senders GNL that are not in the current node's list.
            let gnl_cache = gnl.clone();
            let diff = msg.global_peer_list.difference(&gnl_cache);

            debug!("{} GNL:", &hdr.sender.name);
            for n in &msg.global_peer_list {
                debug!("{}/{}", &n.name, &n.id);
            }

            //Add those elements to the current GNL
            for x in diff { 
                let _res = gnl.insert(x.clone());
                info!("Added peer {}/{} to membership list", x.name, x.id);            

            }
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
        info!("Built HEARTBEAT message for peer: {}, id {:?}", &destination.name, destination.id);
        
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
        info!("Received Heartbeat message from {}, id {:?}", hdr.sender.name, hdr.sender.id);
        
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
        info!("Built Alive message for peer: {}, id {:?}", &hdr.sender.name, hdr.sender.id);
        
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

            debug!("{} GNL:", &hdr.sender.name);
            for n in &msg.global_peer_list {
                debug!("{}/{}", &n.name, &n.id);
            }

            //Add those elements to the current GNL
            for x in diff { 
                gnl.insert(x.clone());
                info!("Added peer {}/{} to membership list", x.name, x.id);            

            }
        } //LOCK : RELEASE : GNL

        Ok(None)
    }

    fn initial_join_scan(&self) -> Result<(), WorkerError> {
        let destination = Peer::new(); //TODO: remove
        let msg = TMembership::create_join_message(self.get_self_peer().clone(), destination)?;
        self.short_radio.broadcast(msg)
    }

    fn heartbeat_thread(&self) -> Result<JoinHandle<Result<(), WorkerError>>, WorkerError> {
        let mut rng = self.rng.clone();
        let sender = self.get_self_peer();
        let short_radio = Arc::clone(&self.short_radio);
        let neighbours = Arc::clone(&self.neighbours);
        let network_members = Arc::clone(&self.network_members);

        let handle = thread::spawn(move || -> Result<(), WorkerError> {
            info!("Starting the heartbeat thread.");

            let sleep_duration = Duration::from_millis(HEARTBEAT_TIMER);

            loop {
                thread::sleep(sleep_duration);
                let nl = Arc::clone(&neighbours);


                let mut selected_peer = Peer::new();
                {   //LOCK : GET : NEAR
                    let near : MutexGuard<HashSet<Peer>> = try!(nl.lock());
                    
                    if near.len() <= 0 {
                        //No peers are nearby. Skip the scan.
                        //[DEADLOCK]Not sure if this releases the acquired lock or not. If any deadlocks occur, look here first.
                        continue;
                    }

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

                //Initial message of the heartbeat handshake.
                let msg = TMembership::create_heartbeat_message(sender.clone(), selected_peer.clone())?; 
                match short_radio.broadcast(msg) {
                    Ok(_) => { },
                    Err(e) => { 
                        error!("Failed to send message to {}", &selected_peer.name);
                    },
                }

                //Print the membership lists at the end of the cycle
                let _ = TMembership::print_membership_list("Neighbors", Arc::clone(&neighbours));
                let _ = TMembership::print_membership_list("Membership", Arc::clone(&network_members));
            }
        });
        Ok(handle)
    }

    fn get_self_peer(&self) -> Peer {
        Peer{ name : self.worker_name.clone(),
              id : self.worker_id.clone(),
              addresses : vec![ AddressType::ShortRange(String::from(self.short_radio.get_address())) ] }
    }

    fn print_membership_list<'a>(name : &'a str, list : Arc<Mutex<HashSet<Peer>>>) -> Result<(), WorkerError> {
        let l = try!(list.lock());

        info!("{}. {} members", name, l.len());
        for m in l.iter() {
            info!("{} : {}", m.name, m.id);
        }
        Ok(())
    }

    fn log_thread_errors(handles : Vec<JoinHandle<Result<(), WorkerError>>>) {    
        let _ = thread::spawn(move || {
            for h in handles {
                let exit = h.join();
                match exit {
                    Ok(_) => { /* All good */ },
                    Err(e) => {
                        error!("{:?}", e);
                    }
                }
            }
        });
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