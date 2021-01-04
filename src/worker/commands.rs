//! This module implements the features to give the worker commands after it has started.

use crate::{MeshSimError, MeshSimErrorKind};
use std::str::FromStr;
use crate::mobility2::{Position, Velocity};

/// Enummeration of all the commands the worker supports
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Commands {
    ///Send a chunk of data to the destination specified
    Send(String, Vec<u8>),
    RegisterWorker{
        w_name: String,
        w_id: String, 
        pos : Position,
        vel: Option<Velocity>,
        dest: Option<Position>,
        sr_address: Option<String>,
        lr_address: Option<String>,
        cmd_address: String,
    },
    Finish,
}

impl FromStr for Commands {
    type Err = MeshSimError;

    fn from_str(s: &str) -> Result<Commands, MeshSimError> {
        let parts: Vec<&str> = s.split_whitespace().collect();

        //Assuming here we can have actions with 0 parameters.
        if !parts.is_empty() {
            match parts[0].to_uppercase().as_str() {
                // "ADD_BCG" => {
                //     if parts.len() < 2 {
                //         //Error out
                //         return Err(WorkerError::Command(format!("Add_bcg needs two parameters: Radio type (short/long) and Broadcast group.")))
                //     }
                //     let radio = parts[1].parse::<RadioTypes>().unwrap();
                //     let bg_name = parts[2].into();
                //     Ok(Commands::Add_bcg(radio, bg_name))
                // },

                // "REM_BCG" => {
                //     if parts.len() < 2 {
                //         //Error out
                //         return Err(WorkerError::Command(format!("Rem_bcg needs two parameters: Radio type (short/long) and Broadcast group.")))
                //     }
                //     let radio = parts[1].parse::<RadioTypes>().unwrap();
                //     let bg_name = parts[2].into();
                //     Ok(Commands::Rem_bcg(radio, bg_name))
                // },
                "SEND" => {
                    if parts.len() < 2 {
                        //Error out
                        let err_msg = String::from(
                            "Send needs two parameters: Radio type (short/long) and data.",
                        );
                        let err = MeshSimError {
                            kind: MeshSimErrorKind::Worker(err_msg),
                            cause: None,
                        };
                        return Err(err);
                    }
                    let destination = parts[1].into();
                    let data = match base64::decode(parts[2].as_bytes()) {
                        Ok(d) => d,
                        Err(e) => {
                            let err_msg = String::from("Failed to decode data from base64 string");
                            let err = MeshSimError {
                                kind: MeshSimErrorKind::Worker(err_msg),
                                cause: Some(Box::new(e)),
                            };
                            return Err(err);
                        }
                    };
                    Ok(Commands::Send(destination, data))
                }

                _ => {
                    let err_msg = format!("Unsupported worker command: {:?}", parts);
                    let err = MeshSimError {
                        kind: MeshSimErrorKind::Worker(err_msg),
                        cause: None,
                    };
                    Err(err)
                }
            }
        } else {
            let err_msg = String::from("Empty command");
            let err = MeshSimError {
                kind: MeshSimErrorKind::Worker(err_msg),
                cause: None,
            };
            Err(err)
        }
    }
}
