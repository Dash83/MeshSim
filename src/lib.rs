#[macro_use]
extern crate serde_derive;
// #[macro_use]
// extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate pretty_assertions;
extern crate serde_json;
extern crate rand;

pub mod master;
pub mod worker;
pub mod logging;