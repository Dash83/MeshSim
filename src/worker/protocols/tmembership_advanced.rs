//! Advanced version of the Toy Membership protocol used to test the 2-radio feature.

extern crate serde_cbor;
extern crate rand;

use worker::protocols::Protocol;
use worker::{WorkerError, Peer, MessageHeader, Radio, Worker};
use std::collections::HashSet;
use std::sync::{Mutex, Arc, MutexGuard};
use self::serde_cbor::de::*;
use self::serde_cbor::ser::*;
use std::thread;
use std::time::Duration;
use self::rand::{StdRng, Rng};

///The main struct for this protocol. Implements the worker::protocol::Protocol trait.
#[derive(Debug)]
pub struct TMembershipAdvanced {
    neighbours : Arc<Mutex<HashSet<Peer>>>,
    network_members : Arc<Mutex<HashSet<Peer>>>,
    short_radio : Arc<Radio>,
    long_radio : Arc<Radio>,
    rng : StdRng,
}

impl TMembershipAdvanced {
    ///Get a new TMembershipAdvanced object
    pub fn new() -> TMembershipAdvanced {
        unimplemented!("Not ready yet!");
    }
}

impl Protocol for TMembershipAdvanced {
    fn handle_message(&self,  msg : MessageHeader) -> Result<Option<MessageHeader>, WorkerError> {
        unimplemented!("Not yet!");
    }

    fn init_protocol(&self) -> Result<Option<MessageHeader>, WorkerError> {
        unimplemented!("Not yet!");
    }
}