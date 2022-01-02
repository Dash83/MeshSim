//*****************
//External crates
//*****************
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate diesel;
#[macro_use]
extern crate diesel_migrations;

//*****************
//Diesel Operations
//*****************
// embed_migrations!("migrations");

//*****************
//Modules declaration
//*****************
pub mod logging;
pub mod master;
pub mod backend;
pub mod tests;
pub mod worker;
pub mod mobility;
pub mod common;

//*****************
//Constants
//*****************
pub const ONE_SECOND_NS: u64 = 1_000_000_000;
pub const ONE_MILLISECOND_NS: u64 = 1_000_000;
pub const ONE_MICROSECOND_NS: u64 = 1_000;

//*****************
//Errors
//*****************
use std::error::Error;
use std::fmt;

/// Error struct for this module
#[derive(Debug)]
pub struct MeshSimError {
    pub cause: Option<Box<dyn Error>>,
    pub kind: MeshSimErrorKind,
}

/// Types of errors produced in this module
#[derive(Debug)]
pub enum MeshSimErrorKind {
    /// Unable to connect to the database
    ConnectionFailure(String),
    /// Failed to execute a given SQL query
    SQLExecutionFailure(String),
    /// Networking related failures
    Networking(String),
    /// Failures related to [de]serializing data
    Serialization(String),
    /// Failures in configuration of the system
    Configuration(String),
    /// Errors from concurrent access to resources
    Contention(String),
    /// Errors from the Worker component
    Worker(String),
    /// Errors from the Master component
    Master(String),
    /// Errors from parsing test specifications
    TestParsing(String),
    ///An error related to network contentiopn
    NetworkContention(String),
}

impl Error for MeshSimError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.cause {
            Some(ref cause) => Some(&**cause),
            None => None,
        }
    }
}

impl fmt::Display for MeshSimError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl fmt::Display for MeshSimErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MeshSimErrorKind::ConnectionFailure(msg) => write!(f, "{}", msg),
            MeshSimErrorKind::SQLExecutionFailure(msg) => write!(f, "{}", msg),
            MeshSimErrorKind::Networking(msg) => write!(f, "{}", msg),
            MeshSimErrorKind::Serialization(msg) => write!(f, "{}", msg),
            MeshSimErrorKind::Configuration(msg) => write!(f, "{}", msg),
            MeshSimErrorKind::Contention(msg) => write!(f, "{}", msg),
            MeshSimErrorKind::Worker(msg) => write!(f, "{}", msg),
            MeshSimErrorKind::Master(msg) => write!(f, "{}", msg),
            MeshSimErrorKind::TestParsing(msg) => write!(f, "{}", msg),
            MeshSimErrorKind::NetworkContention(msg) => write!(f, "{}", msg),
        }
    }
}

impl From<MeshSimErrorKind> for MeshSimError {
    fn from(kind: MeshSimErrorKind) -> MeshSimError {
        MeshSimError { cause: None, kind }
    }
}
