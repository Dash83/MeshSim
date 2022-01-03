/****************** Platform *****************/
#[allow(non_snake_case)]
mod CBR;
mod device_mode;
mod random_waypoint;

/****************** Protocols ****************/
mod aodv;
mod aodvda;
mod gossip_routing;
mod lora_wifi_beacon;
mod naive_routing;
mod reactive_gossip;
#[allow(non_snake_case)]
mod reactive_gossipII;
#[allow(non_snake_case)]
mod reactive_gossipIII;
mod increased_mobility;
mod master;