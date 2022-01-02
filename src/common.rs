use std::iter;
use rand::{RngCore, rngs::StdRng};

use rustc_serialize::hex::*;

///Creates a worker_id based on a random seed.
pub fn generate_worker_id(rng: &mut StdRng) -> String {
    // let mut gen = Worker::rng_from_seed(seed);

    //Vector of 16 bytes set to 0
    let mut key: Vec<u8> = iter::repeat(0u8).take(16).collect();
    //Fill the vector with 16 random bytes.
    rng.fill_bytes(&mut key[..]);
    key.to_hex()
}