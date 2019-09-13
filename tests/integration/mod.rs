
/****************** Platform *****************/
mod device_mode;
#[allow(non_snake_case)]
mod CBR;
mod random_waypoint;

/****************** Protocols ****************/
mod naive_routing;
mod reactive_gossip;
#[allow(non_snake_case)]
mod reactive_gossipII;
#[allow(non_snake_case)]
mod reactive_gossipIII;
mod lora_wifi_beacon;
mod gossip_routing;
mod aodv;