extern crate assert_cli;
extern crate chrono;
extern crate mesh_simulator;
#[cfg(test)]
#[macro_use]
extern crate pretty_assertions;
// #[macro_use]
extern crate lazy_static;
// #[macro_use]
extern crate slog;
#[macro_use]
extern crate peroxide;

use self::chrono::prelude::*;
use self::mesh_simulator::logging::{self, *};
use std::env;

use std::process::{Command, Stdio};

// mod experiments;
// pub mod common;
mod integration;
mod unit;

