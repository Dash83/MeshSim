
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
mod lora_wifi_beacon;
mod gossip_routing;