extern crate toml;

use master::MasterError;
use worker::worker_config::WorkerConfig;
use std::fs::File;
use std::io::Read;
use std::str::FromStr;

///Structure that holds the data of a given test specification.
#[derive(Debug, Deserialize, PartialEq)]
pub struct TestSpec {
    ///Name of the test. For informational purposes only. 
    pub name : String,
    ///Vector of worker configurations for the master to start.
    pub nodes : Vec<WorkerConfig>,
    //TODO: Implement the use of this feature.
    ///Vector of actions for the Master to take.
    pub actions : Vec<String>,
}

impl TestSpec {
    /// This function takes a path to a file that defines a TOML-based
    /// test specification for the Master.
    pub fn parse_test_spec<'a>(file_path : &'a str) -> Result<TestSpec, MasterError> {
        info!("Parsing test file {}.", file_path);
        //Check that the test file passed exists.
        //If it doesn't exist, error out
        let mut file = try!(File::open(file_path));
        let mut file_content = String::new();
        //Not checking bytes read since all we can check without scanning the file is that is not empty.
        //The serialization framework however, will do the appropriate validations.
        let _bytes_read = try!(file.read_to_string(&mut file_content));
        let configuration : TestSpec = try!(toml::from_str(&file_content));
        Ok(configuration)
    }
}

///Structure that holds the data of a given test specification.
#[derive(Debug, Deserialize, PartialEq)]
pub enum TestActions {
    ///This action determines the maximum duration of the test
    EndTest(u64),
}


impl FromStr for TestActions {
    type Err = MasterError;

    fn from_str(s : &str) -> Result<TestActions, MasterError> {
        let parts : Vec<&str> = s.split_whitespace().collect();

        //Assuming here we can have actions with 0 parameters.
        if parts.len() > 0 {
            match parts[0].to_uppercase().as_str() {
                "ENDTEST" => {
                    if parts.len() < 2 {
                        //Error out
                        return Err(MasterError::TestParsing(format!("EndTest needs a u64 time parameter.")))
                    }
                    let time = parts[1].parse::<u64>().unwrap();
                    Ok(TestActions::EndTest(time))
                },
                _ => Err(MasterError::TestParsing(format!("Unsupported Test action: {:?}", parts))),
            }
        } else {
            //Error out
            Err(MasterError::TestParsing(format!("Unsupported Test action: {:?}", parts)))
        }
    }
}