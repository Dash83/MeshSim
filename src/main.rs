extern crate mesh_simulator;
use mesh_simulator::master;

fn main() {
    let master = master::Master{workers : vec![]};

    println!("Master created");
}
