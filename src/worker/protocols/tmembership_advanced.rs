//! Advanced version of the Toy Membership protocol used to test the 2-radio feature.

extern crate serde_cbor;
extern crate rand;

use worker::protocols::Protocol;
use worker::{WorkerError, Peer, MessageHeader, Worker};
use worker::radio::*;
use std::collections::HashMap;
use std::sync::{Mutex, Arc, MutexGuard};
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use self::rand::{Rng, StdRng, RngCore};

const HEARTBEAT_TIMER : u64 = 3000; //3000 ms

///The main struct for this protocol. Implements the worker::protocol::Protocol trait.
#[derive(Debug)]
pub struct TMembershipAdvanced {
    neighbours_short : Arc<Mutex<HashMap<String, Peer>>>,
    neighbours_long : Arc<Mutex<HashMap<String, Peer>>>,
    network_members : Arc<Mutex<HashMap<String, Peer>>>,
    short_radio : Arc<Radio>,
    long_radio : Arc<Radio>,
    rng : StdRng,
    worker_name : String,
    worker_id : String,
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
    global_peer_list : HashMap<String, Peer>,
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
    global_peer_list : HashMap<String, Peer>,
}

impl TMembershipAdvanced {
    ///Get a new TMembershipAdvanced object
    pub fn new(sr : Arc<Radio>, lr : Arc<Radio>, seed : u32, id : String, name : String) -> TMembershipAdvanced {
        let n_short = Arc::new(Mutex::new(HashMap::new()));
        let n_long = Arc::new(Mutex::new(HashMap::new()));
        
        let mut members = HashMap::new();
        let p = Peer{ id: id.clone(),
                      name : name.clone(),
                      short_address : Some(sr.get_address().into()),
                      long_address : Some(lr.get_address().into()) };
        members.insert(id.clone(), p);
        let m = Arc::new(Mutex::new(members));
        let rng = Worker::rng_from_seed(seed);

        TMembershipAdvanced{ neighbours_short : n_short,
                             neighbours_long : n_long,
                             network_members : m,
                             short_radio : sr,
                             long_radio : lr,
                             rng : rng,
                             worker_name : name,
                             worker_id : id }
    }
}

impl Protocol for TMembershipAdvanced {
    fn handle_message(&self, mut header : MessageHeader, r_type : RadioTypes) -> Result<Option<MessageHeader>, WorkerError> {
        let data = match header.payload.take() {
            Some(d) => { d },
            None => {
                warn!("Messaged received from {:?} had empty payload.", header.sender);
                return Ok(None)
            }
        };
        
        let msg = try!(TMembershipAdvanced::build_protocol_message(data));
        let nl = Arc::clone(&self.neighbours_short);
        let fl = Arc::clone(&self.neighbours_long);
        let gl = Arc::clone(&self.network_members);
        let me = self.get_self_peer();
        TMembershipAdvanced::handle_message_internal(header, msg, me, nl, fl, gl, r_type)
    }

    fn init_protocol(&self) -> Result<Option<MessageHeader>, WorkerError>{
        //Perform initial scan
        let handles = try!(self.initial_join_scan());
        //Create a thread to join on the threads and log any errors.
        TMembershipAdvanced::log_thread_errors(handles);

        // Start thread for heartbeat messages.
        let _res = try!(self.heartbeat_thread());
        
        Ok(None)
    }

    fn send(&self, destination : String, data : Vec<u8>) -> Result<(), WorkerError> {
        unimplemented!()
    }

}

impl TMembershipAdvanced {
    //TODO: Fix the scan process
    fn initial_join_scan(&self) -> Result<Vec<JoinHandle<Result<(), WorkerError>>>, WorkerError> {
        let mut handles = Vec::new();
        // //Perform radio-scan.
        // let nearby_peers = try!(self.short_radio.scan_for_peers());
        // let mut far_peers = try!(self.long_radio.scan_for_peers());

        // //In a lot of cases, the devices detected by the Long-range radio will have a large intersection
        // //with those detected by the short-range one (or even be a superset of it). So, we remove those elements from
        // //those scanned by the long-range radio to not dupplicate the join handshakes
        // debug!("Far_peers.len(): {}", far_peers.len());
        // for (key, _val) in nearby_peers.iter() {
        //     if far_peers.contains_key(key) {
        //         debug!("Removed peer {} already present in near_peers", &key);
        //         let _val = far_peers.remove(key);
        //     }
        // }
        // debug!("Far_peers.len(): {}", far_peers.len());

        // //Send join messages to all scanned devices with short_range radio.
        // for (key, val) in nearby_peers.iter() {
        //     //Get all required resources
        //     let sender = self.get_self_peer();
        //     let short_radio = Arc::clone(&self.short_radio);
        //     let ns = Arc::clone(&self.neighbours_short);
        //     let nl = Arc::clone(&self.neighbours_long);
        //     let network_members = Arc::clone(&self.network_members);
        //     let name = val.0.clone();
        //     let address = val.1.clone();
        //     let id = key.clone();

        //     let jh = try!(TMembershipAdvanced::new_handshake_thread(sender, address, name, id, short_radio, RadioTypes::ShortRange, 
        //                                                             ns, nl, network_members));
        //     handles.push(jh);
        // }

        // //Send join messages to all scanned devices with long_range radio.
        // for (key, val) in far_peers.iter() {
        //     //Get all required resources
        //     let sender = self.get_self_peer();
        //     let long_radio = Arc::clone(&self.long_radio);
        //     let ns = Arc::clone(&self.neighbours_short);
        //     let nl = Arc::clone(&self.neighbours_long);
        //     let network_members = Arc::clone(&self.network_members);
        //     let name = val.0.clone();
        //     let address = val.1.clone();
        //     let id = key.clone();

        //     let jh = try!(TMembershipAdvanced::new_handshake_thread(sender, address, name, id, long_radio, RadioTypes::LongRange, 
        //                                                             ns, nl, network_members));
        //     handles.push(jh);
        // }
    
        Ok(handles)
    }

    fn handle_message_internal(hdr : MessageHeader, msg : Messages, me : Peer,
                               neighbours_short : Arc<Mutex<HashMap<String, Peer>>>,
                               neighbours_long : Arc<Mutex<HashMap<String, Peer>>>,
                               network_members : Arc<Mutex<HashMap<String, Peer>>>,
                               r_type : RadioTypes) -> Result<Option<MessageHeader>, WorkerError> {
        let neighbours = match r_type {
            RadioTypes::ShortRange => neighbours_short,
            RadioTypes::LongRange => neighbours_long,
        };

        let response = match msg {
                            Messages::Join(m) => {
                                TMembershipAdvanced::process_join_message(hdr, m, me, neighbours, network_members)
                            },
                            Messages::Ack(m) => {
                                TMembershipAdvanced::process_ack_message(hdr, m, neighbours, network_members)
                            },
                            Messages::Heartbeat(m) => {
                                TMembershipAdvanced::process_heartbeat_message(hdr, m, neighbours, network_members)
                            },
                            Messages::Alive(m) => {
                                TMembershipAdvanced::process_alive_message(hdr, m, neighbours, network_members)
                            }
                       };
        response
    }

    /// The first message of the protocol. 
    /// When to send: At start-up, after doing a scan of nearby peers.
    /// Receiver: All Nearby peers.
    /// Payload: Self Peer object.
    /// Actions: None.
    fn create_join_message(sender : Peer, destination : Peer) -> Result<MessageHeader, WorkerError> {
        let data = JoinMessage;
        let join_msg = Messages::Join(data);
        let payload = try!(to_vec(&join_msg));
        info!("Built JOIN message for peer: {}, id {}", &destination.name, destination.id);
        
        //Build the message header that's ready for sending.
        let msg = MessageHeader{ sender : sender, 
                                 destination : destination, 
                                 payload : Some(payload) };
        Ok(msg)
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
        info!("Built HEARTBEAT message for peer: {}, id {}", &destination.name, destination.id);
        
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
                            neighbours : Arc<Mutex<HashMap<String, Peer>>>,
                            network_members : Arc<Mutex<HashMap<String, Peer>>>) -> Result<Option<MessageHeader>, WorkerError> {
        info!("Received JOIN message from {}", hdr.sender.name);

        {
            //LOCK : GET : NEAR
            let mut near = try!(neighbours.lock());
            
            //Presumably, the sender is a new node joining. If already in our neighbour list, this is a NOP.
            let _res = near.insert(hdr.sender.id.clone(), hdr.sender.clone());
        }   //LOCK : RELEASE : NEAR

        {
            //LOCK : GET : GNL
            let mut gnl = try!(network_members.lock());

            //Build the internal message of the response
            let msg_data = AckMessage{ global_peer_list : gnl.clone()};

            //Add the sender to our own GNL.
            let _res = gnl.insert(hdr.sender.id.clone(), hdr.sender.clone());

            //Build the ACK message for the sender
            let ack_msg = Messages::Ack(msg_data);
            let payload = try!(to_vec(&ack_msg));
            info!("Built ACK message for sender: {}, id {}", &hdr.sender.name, hdr.sender.id);
            
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
                            neighbours : Arc<Mutex<HashMap<String, Peer>>>,
                            network_members : Arc<Mutex<HashMap<String, Peer>>>) -> Result<Option<MessageHeader>, WorkerError> {        
        info!("Received ACK message from {}", hdr.sender.name);
        {
            //LOCK : GET : NEAR
            let mut near = try!(neighbours.lock());
            
            let _res = near.insert(hdr.sender.id.clone(), hdr.sender.clone());
            info!("Added peer {}/{} to neighbours list", &hdr.sender.name, &hdr.sender.id);
        }   //LOCK : RELEASE : NEAR

        {
            //LOCK : GET : GNL
            let mut gnl = try!(network_members.lock());

            debug!("{} GNL:", &hdr.sender.name);
            for (key, p) in &msg.global_peer_list {
                debug!("{}/{}", &p.name, &key);
            }

            //Get the peers (if any) in the senders GNL that are not in the current node's list.
            for (key, p) in &msg.global_peer_list {
                if !gnl.contains_key(key) {
                    let _old = gnl.insert(key.clone(), p.clone());
                    info!("Added peer {}/{} to membership list", &p.name, &key);
                }
            }
        } //LOCK : RELEASE : GNL

        Ok(None) //Protocol handshake ends here.
    }

    /// A response to a heartbeat message.
    /// When to send: After receiving a heartbeat message.
    /// Sender: A nearby peer in range.
    /// Payload: global node list.
    /// Actions:
    ///   1. Construct an alive message with this peer's GNL and send it.
    ///   2. Add the sender to the neighbor list (if not there already)
    fn process_heartbeat_message(hdr : MessageHeader, msg : HeartbeatMessage,
                                 neighbours : Arc<Mutex<HashMap<String, Peer>>>,
                                 network_members : Arc<Mutex<HashMap<String, Peer>>>) -> Result<Option<MessageHeader>, WorkerError> {
        info!("Received Heartbeat message from {}, id {}", hdr.sender.name, hdr.sender.id);
        
        {
            //LOCK : GET : NL
            let mut nl = try!(neighbours.lock());

            //If the sender peer is not in our current neihbor list, add it.
            let _old = nl.insert(hdr.sender.id.clone(), hdr.sender.clone());
        } //LOCK : RELEASE : NL

        //Obtain a copy of our current GNL that's necesary for the response message.
        let gnl_cache : HashMap<String, Peer> = match network_members.lock() { //LOCK : GET : GNL
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
        info!("Built Alive message for peer: {}, id {}", &hdr.sender.name, hdr.sender.id);
        
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
                             _neighbours : Arc<Mutex<HashMap<String, Peer>>>,
                             network_members : Arc<Mutex<HashMap<String, Peer>>>) -> Result<Option<MessageHeader>, WorkerError> {
        info!("Received Alive message from {}, id {}.", hdr.sender.name, hdr.sender.id);

        {
            //LOCK : GET : GNL
            let mut gnl = try!(network_members.lock());

            debug!("{} GNL:", &hdr.sender.name);
            for (key, p) in &msg.global_peer_list {
                debug!("{}/{}", &p.name, &key);
            }

            //Get the peers (if any) in the senders GNL that are not in the current node's list.
            for (key, p) in &msg.global_peer_list {
                if !gnl.contains_key(key) {
                    let _old = gnl.insert(key.clone(), p.clone());
                    info!("Added peer {}/{} to membership list", &p.name, &key);
                }
            }
        } //LOCK : RELEASE : GNL

        Ok(None)
    }

    fn new_handshake_thread(sender : Peer, address : String, name : String, id : String, 
                            r : Arc<Radio>, r_type : RadioTypes, 
                            neighbours_short : Arc<Mutex<HashMap<String, Peer>>>,
                            neighbours_long : Arc<Mutex<HashMap<String, Peer>>>, 
                            network_members : Arc<Mutex<HashMap<String, Peer>>>,) -> Result<JoinHandle<Result<(), WorkerError>>, WorkerError> {
        //Destination peer
        let p = Peer { name : name,
                      id : id,
                      short_address : Some(address.clone()),
                      long_address : None };

        let h = thread::spawn(move || -> Result<(), WorkerError> {
            //Create join message
            // let msg = TMembershipAdvanced::create_join_message(sender.clone(), p)?;
            
            // let _res = r.broadcast(msg)?;

            // //Connect to the remote peer
            // let mut c = try!(r.connect(address)); 
            // //Send initial message
            // let _res = try!(c.send_msg(msg));

            // //We will now enter a read/response loop until the protocol finishes.
            // loop {
            //     //Read the data from the unix socket
            //     let recv_msg = try!(c.read_msg());
            //     //Try to decode the data into a message.
            //     let response = match recv_msg {
            //         Some(mut hdr) => { 
            //             let data = hdr.payload.take().unwrap_or(vec![]);
            //             let msg = try!(TMembershipAdvanced::build_protocol_message(data));
            //             try!(TMembershipAdvanced::handle_message_internal(hdr, 
            //                                                               msg, 
            //                                                               sender.clone(), 
            //                                                               Arc::clone(&neighbours_short),
            //                                                               Arc::clone(&neighbours_long),  
            //                                                               Arc::clone(&network_members),
            //                                                               r_type) )
            //         },
            //         None => None,
            //     };
            //     //let response = try!(self.handle_message(recv_msg));
            //     match response {
            //         Some(resp_msg) => { 
            //             let _res = try!(c.send_msg(resp_msg));
            //         },
            //         None => {
            //             //End of protocol sequence.
            //             break;
            //         }
            //     }
            // }
            Ok(())
        });
        Ok(h)
    }

   fn heartbeat_thread(&self) -> Result<JoinHandle<Result<(), WorkerError>>, WorkerError> {
        let mut rng = self.rng.clone();
        let sender = self.get_self_peer();
        let short_radio = Arc::clone(&self.short_radio);
        let long_radio = Arc::clone(&self.long_radio);
        let sn = Arc::clone(&self.neighbours_short);
        let ln = Arc::clone(&self.neighbours_long);
        let network_members = Arc::clone(&self.network_members);

        let handle = thread::spawn(move || -> Result<(), WorkerError> {
            info!("Starting the heartbeat thread.");

            let sleep_duration = Duration::from_millis(HEARTBEAT_TIMER);

            loop {
                thread::sleep(sleep_duration);
                let mut random_choice = rng.next_u32();
                let _res = TMembershipAdvanced::start_heartbeat_handshake(sender.clone(), 
                                                                          random_choice, 
                                                                          Arc::clone(&short_radio), 
                                                                          RadioTypes::ShortRange,
                                                                          Arc::clone(&sn), 
                                                                          Arc::clone(&ln),
                                                                          Arc::clone(&network_members) );
                random_choice = rng.next_u32();
                let _res = TMembershipAdvanced::start_heartbeat_handshake(sender.clone(), 
                                                                          random_choice, 
                                                                          Arc::clone(&long_radio),
                                                                          RadioTypes::LongRange,
                                                                          Arc::clone(&sn),
                                                                          Arc::clone(&ln), 
                                                                          Arc::clone(&network_members) );
                //Print the membership lists at the end of the cycle
                let _ = TMembershipAdvanced::print_membership_list("Near neighbors", Arc::clone(&sn));
                let _ = TMembershipAdvanced::print_membership_list("Far neighbors", Arc::clone(&ln));
                let _ = TMembershipAdvanced::print_membership_list("Membership", Arc::clone(&network_members));
            }
        });
        Ok(handle)
    }

    fn start_heartbeat_handshake(sender : Peer,  random_choice : u32, radio : Arc<Radio>, r_type : RadioTypes,
                                 neighbours_short : Arc<Mutex<HashMap<String, Peer>>>,
                                 neighbours_long : Arc<Mutex<HashMap<String, Peer>>>, 
                                 network_members : Arc<Mutex<HashMap<String, Peer>>>) -> Result<(), WorkerError> {
        let nl = Arc::clone(&neighbours_short);
        let fl = Arc::clone(&neighbours_long);

        let mut selected_peer = Peer::new();
        {   //LOCK : GET : NEAR
            let near : MutexGuard<HashMap<String, Peer>> = try!(nl.lock());
            
            if near.len() <= 0 {
                //No peers are nearby. Skip the scan.
                //[DEADLOCK]Not sure if this releases the acquired lock or not. If any deadlocks occur, look here first.
                return Ok(())
            }

            //Select random peer
            let p_index : u32 = random_choice % near.len() as u32;

            let mut i = 0;
            for (id, p) in near.iter() {
                if i == p_index{
                    selected_peer = p.clone();
                }
                i += 1;
            }

            if i == 0 {
                //Empty neighbor list
                return Ok(())
            }
        }   //LOCK : RELEASE : NEAR

        //Initial message of the heartbeat handshake.
        let msg = TMembershipAdvanced::create_heartbeat_message(sender.clone(), selected_peer.clone())?; 
        match radio.broadcast(msg) {
            Ok(_) => { },
            Err(e) => { 
                error!("Failed to send message to {}", &selected_peer.name);
            },
        }

        //Print the membership lists at the end of the cycle
        let _ = TMembershipAdvanced::print_membership_list("Neighbors short", Arc::clone(&nl));
        let _ = TMembershipAdvanced::print_membership_list("Neighbors long", Arc::clone(&fl));
        let _ = TMembershipAdvanced::print_membership_list("Membership", Arc::clone(&network_members));

        Ok(())
    }

    fn build_protocol_message(data : Vec<u8>) -> Result<Messages, serde_cbor::Error> {
        let res : Result<Messages, serde_cbor::Error> = from_slice(data.as_slice());
        res
    }

    fn get_self_peer(&self) -> Peer {
        Peer{ name : self.worker_name.clone(),
              id : self.worker_id.clone(),
              short_address : Some(self.short_radio.get_address().into()),
              long_address : None, }
    }

    fn print_membership_list<'a>(name : &'a str, list : Arc<Mutex<HashMap<String, Peer>>>) -> Result<(), WorkerError> {
        let l = try!(list.lock());

        info!("{}. {} members:", name, l.len());
        for (id, p) in l.iter() {
            info!("{} : {}", p.name, id);
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

