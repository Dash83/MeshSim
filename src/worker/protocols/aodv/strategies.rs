use crate::worker::protocols::aodv::{RouteResponseMessage, RouteRequestMessage, RouteTableEntry};
use crate::worker::MessageHeader;
use std::cmp::min;

pub trait AodvStrategy: std::fmt::Debug + Send + Sync {
    fn should_update_route(
        &self,
        hdr: &MessageHeader,
        msg: &RouteResponseMessage,
        entry: &mut RouteTableEntry
    ) -> bool;

    fn update_route_request_message(
        &self,
        hdr: &MessageHeader,
        msg: &mut RouteRequestMessage,
    ) -> ();

    fn update_route_response_message(
        &self,
        hdr: &MessageHeader,
        msg: &mut RouteResponseMessage,
    ) -> ();

    fn update_route_entry(
        &self,
        hdr: &MessageHeader,
        msg: &RouteResponseMessage,
        entry: &mut RouteTableEntry,
    ) -> ();
}

#[derive(Debug)]
pub struct AodvNormal {
}

#[derive(Debug)]
pub struct AodvDistanceAdjusted {
    pub algorithm: i32,
    pub max_loss: f64,
}

impl AodvStrategy for AodvNormal {
    fn should_update_route(
        &self,
        _hdr: &MessageHeader,
        msg: &RouteResponseMessage,
        entry: &mut RouteTableEntry
    ) -> bool {
        msg.route_cost < entry.route_cost
    }

    fn update_route_request_message(
        &self,
        _hdr: &MessageHeader,
        msg: &mut RouteRequestMessage,
    ) -> () {
        msg.route_cost += 1.0f64;
    }

    fn update_route_response_message(
        &self,
        hdr: &MessageHeader,
        msg: &mut RouteResponseMessage,
    ) -> () {
        msg.route_cost += 1.0f64;
    }

    fn update_route_entry(
        &self,
        _hdr: &MessageHeader,
        msg: &RouteResponseMessage,
        entry: &mut RouteTableEntry,
    ) -> () {
        entry.route_cost = msg.route_cost
    }
}

impl AodvStrategy for AodvDistanceAdjusted {
    fn should_update_route(
        &self,
        _hdr: &MessageHeader,
        msg: &RouteResponseMessage,
        entry: &mut RouteTableEntry
    ) -> bool {
        msg.route_cost < entry.route_cost
    }

    fn update_route_request_message(
        &self,
        hdr: &MessageHeader,
        msg: &mut RouteRequestMessage,
    ) -> () {
        if self.algorithm == 0 {
            let loss = hdr.signal_loss.clamp(hdr.signal_loss, self.max_loss);
            msg.route_cost += loss;
        } else {
            let loss = hdr.signal_loss.clamp(hdr.signal_loss, self.max_loss);
            msg.route_cost += 1.0f64 / ((self.max_loss - loss) / self.max_loss);
        }
    }

    fn update_route_response_message(
        &self,
        hdr: &MessageHeader,
        msg: &mut RouteResponseMessage,
    ) -> () {
        if self.algorithm == 0 {
            let loss = hdr.signal_loss.clamp(hdr.signal_loss, self.max_loss);
            msg.route_cost += loss;
        } else {
            let loss = hdr.signal_loss.clamp(hdr.signal_loss, self.max_loss);
            msg.route_cost += 1.0f64 / ((self.max_loss - loss) / self.max_loss);
        }
    }

    fn update_route_entry(
        &self,
        _hdr: &MessageHeader,
        msg: &RouteResponseMessage,
        entry: &mut RouteTableEntry,
    ) -> () {
        entry.route_cost = msg.route_cost;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logging;
    use std::env;
    use std::fs::File;
    use std::io::Read;

    
}