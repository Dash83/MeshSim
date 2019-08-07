//! This module implements the features to give the worker commands after it has started.

use crate::worker::WorkerError;
use std::str::FromStr;

/// Enummeration of all the commands the worker supports
#[derive(Debug)]
pub enum Commands {
    ///Send a chunk of data to the destination specified
    Send(String, Vec<u8>),
}

impl FromStr for Commands {
    type Err = WorkerError;

    fn from_str(s: &str) -> Result<Commands, WorkerError> {
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
                        return Err(WorkerError::Command(String::from(
                            "Send needs two parameters: Radio type (short/long) and data.",
                        )));
                    }
                    let destination = parts[1].into();
                    let data = match base64::decode(parts[2].as_bytes()) {
                        Ok(d) => d,
                        Err(e) => return Err(WorkerError::Command(format!("{}", e))),
                    };
                    Ok(Commands::Send(destination, data))
                }

                _ => Err(WorkerError::Command(format!(
                    "Unsupported worker command: {:?}",
                    parts
                ))),
            }
        } else {
            //Error out
            Err(WorkerError::Command(format!(
                "Unsupported worker command: {:?}",
                parts
            )))
        }
    }
}
