#[macro_use]
extern crate serde_derive;
// #[macro_use]
// extern crate log;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate slog;

pub mod master;
pub mod worker;
pub mod logging;