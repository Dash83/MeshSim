extern crate toml;

use super::workloads::*;
use crate::master::MobilityModels;
use crate::worker::worker_config::WorkerConfig;
use crate::{MeshSimError, MeshSimErrorKind};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::str::FromStr;
use crate::worker::protocols::Protocols;
use std::default::Default;

/// Struct to keep the area of the simulation
#[derive(Debug, Deserialize, Serialize, PartialEq, Default)]
pub struct Area {
    /// Horizontal measure of the test area
    pub width: f64,
    /// Vertical measure of the test area
    pub height: f64,
}

///Structure that holds the data of a given test specification.
#[derive(Debug, Deserialize, Serialize, PartialEq, Default)]
pub struct TestSpec {
    ///Name of the test. For informational purposes only.
    pub name: String,
    /// Duration of the test in milliseconds.
    pub duration: u64,
    /// Vector of actions for the Master to take.
    pub actions: Vec<String>,
    /// Size of the simulation area.
    #[serde(flatten)]
    pub area_size: Area,
    /// The protocol that this test will run.
    pub protocol: Protocols,
    /// The pattern of mobility the workers will follow
    pub mobility_model: Option<MobilityModels>,
    /// Collection of worker configurations for the master to start at the beginning of the test.
    pub initial_nodes: HashMap<String, WorkerConfig>,
    /// Collection of available worker configurations that the master may start at any time during
    /// the test.
    pub available_nodes: HashMap<String, WorkerConfig>,
}

impl TestSpec {
    /// This function takes a path to a file that defines a TOML-based
    /// test specification for the Master.
    pub fn parse_test_spec(file_path: &str) -> Result<TestSpec, MeshSimError> {
        let mut file_content = String::new();

        //Check that the test file passed exists.
        //If it doesn't exist, error out
        let mut file = File::open(file_path).map_err(|e| {
            let err_msg = String::from("Failed to open test file");
            MeshSimError {
                kind: MeshSimErrorKind::TestParsing(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

        //Not checking bytes read since all we can check without scanning the file is that is not empty.
        //The serialization framework however, will do the appropriate validations.
        let _bytes_read = file.read_to_string(&mut file_content).map_err(|e| {
            let err_msg = String::from("Failed to read test file content");
            MeshSimError {
                kind: MeshSimErrorKind::TestParsing(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;

        let configuration: TestSpec = toml::from_str(&file_content).map_err(|e| {
            let err_msg = String::from("Error parsing test spec");
            MeshSimError {
                kind: MeshSimErrorKind::TestParsing(err_msg),
                cause: Some(Box::new(e)),
            }
        })?;
        Ok(configuration)
    }

    ///Creates new empty configuration
    pub fn new() -> TestSpec {
        TestSpec {
            name: String::from(""),
            duration: 0,
            area_size: Area {
                width: 0.0,
                height: 0.0,
            },
            mobility_model: None,
            actions: vec![],
            protocol : Default::default(),
            initial_nodes: HashMap::new(),
            available_nodes: HashMap::new(),
        }
    }
}

///Structure that holds the data of a given test specification.
#[derive(Debug, Deserialize, PartialEq)]
pub enum TestActions {
    ///This action determines the maximum duration of the test
    EndTest(u64),
    ///Adds the indicated node from the available nodes pool at the indicated time offset.
    AddNode(String, u64),
    ///Kills the indicated node at the specified time offset.
    KillNode(String, u64),
    ///Sends a Ping packet from src to dst. The underlying protocol determines how the data is forwarded.
    Ping(String, String, u64),
    ///Adds a new transmission source
    AddSource(String, SourceProfiles, u64),
}

impl FromStr for TestActions {
    type Err = MeshSimError;

    fn from_str(s: &str) -> Result<TestActions, MeshSimError> {
        let parts: Vec<&str> = s.split_whitespace().collect();

        //Assuming here we can have actions with 0 parameters.
        if !parts.is_empty() {
            match parts[0].to_uppercase().as_str() {
                "END_TEST" => {
                    if parts.len() < 2 {
                        //Error out
                        let msg = String::from("End_Test needs a u64 time parameter.");
                        let err = MeshSimError {
                            kind: MeshSimErrorKind::TestParsing(msg),
                            cause: None,
                        };
                        return Err(err);
                    }
                    let time = parts[1].parse::<u64>().unwrap();
                    Ok(TestActions::EndTest(time))
                }
                "ADD_NODE" => {
                    if parts.len() < 3 {
                        //Error out
                        let msg =
                            String::from("Add_Node needs a worker name and a u64 time parameter.");
                        let err = MeshSimError {
                            kind: MeshSimErrorKind::TestParsing(msg),
                            cause: None,
                        };
                        return Err(err);
                    }
                    let node_name = parts[1].to_owned();
                    let time = parts[2].parse::<u64>().unwrap();

                    Ok(TestActions::AddNode(node_name, time))
                }
                "KILL_NODE" => {
                    if parts.len() < 3 {
                        //Error out
                        let msg =
                            String::from("Kill_Node needs a worker name and a u64 time parameter.");
                        let err = MeshSimError {
                            kind: MeshSimErrorKind::TestParsing(msg),
                            cause: None,
                        };
                        return Err(err);
                    }
                    let node_name = parts[1].to_owned();
                    let time = parts[2].parse::<u64>().unwrap();

                    Ok(TestActions::KillNode(node_name, time))
                }
                "PING" => {
                    if parts.len() < 4 {
                        //Error out
                        let msg =
                            String::from("Ping needs a source, destination and a time parameters.");
                        let err = MeshSimError {
                            kind: MeshSimErrorKind::TestParsing(msg),
                            cause: None,
                        };
                        return Err(err);
                    }
                    let src = parts[1].to_owned();
                    let dst = parts[2].to_owned();
                    let time = parts[3].parse::<u64>().unwrap();

                    Ok(TestActions::Ping(src, dst, time))
                }
                "ADD_SOURCE" => {
                    if parts.len() < 7 {
                        //Error out
                        let msg = String::from(
                            "Add_Source needs the following parameters:
                                                *Source
                                                *Destination
                                                *Packets per Second
                                                *Packet size
                                                *Duration
                                                *Start time",
                        );
                        let err = MeshSimError {
                            kind: MeshSimErrorKind::TestParsing(msg),
                            cause: None,
                        };
                        return Err(err);
                    }
                    let src = parts[1].to_owned();
                    let dst = parts[2].to_owned();
                    let pps = parts[3].parse::<usize>().unwrap();
                    let psize = parts[4].parse::<usize>().unwrap();
                    let duration = parts[5].parse::<u64>().unwrap();
                    let time = parts[6].parse::<u64>().unwrap();
                    let profile = SourceProfiles::CBR(dst, pps, psize, duration);

                    Ok(TestActions::AddSource(src, profile, time))
                }
                _ => {
                    let msg = format!("Unsupported Test action: {:?}", parts);
                    Err(MeshSimError {
                        kind: MeshSimErrorKind::TestParsing(msg),
                        cause: None,
                    })
                }
            }
        } else {
            //Error out
            let msg = String::from("Empty test action");
            let err = MeshSimError {
                kind: MeshSimErrorKind::TestParsing(msg),
                cause: None,
            };
            Err(err)
        }
    }
}
