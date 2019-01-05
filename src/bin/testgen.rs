extern crate mesh_simulator;
extern crate toml;
extern crate rand;

use mesh_simulator::master::test_specification::*;
use mesh_simulator::worker::worker_config::*;
use mesh_simulator::worker::protocols::Protocols;
use mesh_simulator::worker;
use mesh_simulator::worker::mobility::Position;

use std::error;
use std::fmt;
use std::io;
use std::str::FromStr;
use std::path::PathBuf;
use std::fs::File;
use std::io::Write;
use rand::{thread_rng, Rng, RngCore};
use rand::distributions::Uniform;

const DEFAULT_FILE_NAME : &'static str = "untitled";
const DEFAULT_NODE_NAME : &'static str = "node";

#[derive(Debug)]
struct TestBasics {
    pub test_name : String,
    pub end_time : u64,
    pub protocol : Protocols,
    pub side_size : f64,
    pub work_dir : String,
}

impl TestBasics {
    pub fn new() -> TestBasics {
        TestBasics{ test_name : String::from(""),
                    end_time : 0,
                    protocol : Protocols::TMembership,
                    side_size : 0.0,
                    work_dir : String::from("") }
    }
}
#[derive(Debug)]
enum Commands {
    ///Finish this configuration and write test file to disk
    finish,
    ///Add nodes to the test. Params: Type of nodes [Initial/Available], Number of nodes.
    add_nodes(String, usize),
    
    // add_action(TestActions),
}

impl FromStr for Commands {
    type Err = Errors;

    fn from_str(s : &str) -> Result<Commands, Errors> {
        let parts : Vec<&str> = s.split_whitespace().collect();

        //Assuming here we can't have actions with 0 parameters.
        if parts.len() > 0 {
            match parts[0].to_uppercase().as_str() {
                "FINISH" => {

                    Ok(Commands::finish)
                },
                "ADD_NODES" => {
                   if parts.len() < 3 {
                        //Error out
                        return Err(Errors::TestParsing(format!("Add_nodes requires 2 params: node_type [Initial/Available] and number")))
                    }
                    let node_type = parts[1].into();
                    let number : usize = match parts[2].parse() {
                        Ok(n) => n,
                        Err(e) => return Err(Errors::TestParsing(format!("{}", e)))
                    };

                    Ok(Commands::add_nodes(node_type, number))
                },

                _ => Err(Errors::TestParsing(format!("Unsupported command: {:?}", parts))),
            }
        } else {
            //Error out
            Err(Errors::TestParsing(format!("Unsupported command: {:?}", parts)))
        }
    }
}
//region Error definitions
#[derive(Debug)]
enum Errors {
    TestParsing(String),
    IO(io::Error),
    Serialization(toml::ser::Error),
}

impl error::Error for Errors {
    fn description(&self) -> &str {
        match *self {
            // CLIError::SetLogger(ref err) => err.description(),
            // CLIError::IO(ref err) => err.description(),
            // CLIError::Master(ref err) => err.description(),
            Errors::TestParsing(ref err_str) => err_str,
            Errors::IO(ref err) => err.description(),
            Errors::Serialization(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            // CLIError::SetLogger(ref err) => Some(err),
            // CLIError::IO(ref err) => Some(err),
            // CLIError::Master(ref err) => Some(err),
            Errors::TestParsing(_) => None,
            Errors::IO(ref err) => Some(err),
            Errors::Serialization(ref err) => Some(err),
        }
    }
}

impl fmt::Display for Errors {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            // CLIError::SetLogger(ref err) => write!(f, "SetLogger error: {}", err),
            // CLIError::IO(ref err) => write!(f, "IO error: {}", err),
            // CLIError::Master(ref err) => write!(f, "Error in Master layer: {}", err),
            Errors::TestParsing(ref err_str) => write!(f, "Error creating test specification: {}", err_str),
            Errors::IO(ref err) => write!(f, "IO error: {}", err),
            Errors::Serialization(ref err) => write!(f, "Serialization error: {}", err),
        }
    }

}

//Error conversions
impl From<io::Error> for Errors {
    fn from(err : io::Error) -> Errors {
        Errors::IO(err)
    }
}

//Error conversions
impl From<toml::ser::Error> for Errors {
    fn from(err : toml::ser::Error) -> Errors {
        Errors::Serialization(err)
    }
}
//endregion

fn start_command_loop(data : TestBasics) -> Result<(), Errors> {
    let mut input = String::new();
    let mut spec = TestSpec::new();
    let mut stdout = io::stdout();

    println!("Input commands to continue building the test spec.");
    println!("Input ? for a list of commands or \"finish\" for writing the current configuration to file.");
    loop {
        print!("> ");
        let _res = stdout.flush()?;
        match io::stdin().read_line(&mut input) {
            Ok(_bytes) => {
                match input.parse::<Commands>() {
                    Ok(command) => {
                        println!("Command received: {:?}", &command);
                        let finish = match process_command(command, &mut spec, &data) {
                            Ok(f) => f,
                            Err(e) => {
                                println!("Error executing command: {}", e);
                                false
                            }
                        };
                        
                        if finish {
                            break;
                        }
                    },
                    Err(e) => { 
                        println!("Error parsing command: {}", e);
                    },
                }
            }
            Err(error) => { 
                println!("{}", error);
            }
        }
        input.clear();
    }
    Ok(())
}

fn process_command(com : Commands, spec : &mut TestSpec, data : &TestBasics) -> Result<bool, Errors> {
    match com {
        Commands::finish => {
            command_finish(spec, data)
        }
        Commands::add_nodes(node_type, num) => {
            command_add_nodes(node_type, num, spec, data)
        }
    }
}

fn command_finish(spec : &mut TestSpec, data : &TestBasics) -> Result<bool, Errors> {
    //Add end_test action
    spec.actions.push(format!("End_Test {}", data.end_time));

    let mut p = PathBuf::from(".");
    //Determine file name to write
    spec.name = data.test_name.clone();
    let file_name = match spec.name.eq("") {
        true => format!("{}.toml", DEFAULT_FILE_NAME),
        false => format!("{}.toml", &spec.name),
    };
    p.push(file_name);
    //let canon = p.canonicalize().expect("Invalid file path");

    let mut input = String::new();
    println!("Current spec: {:?}", &spec);
    println!("Writing {:?} to disk. Press enter to confirm, or provide a new path.", &p);
    let bytes = io::stdin().read_line(&mut input)?;
    
    //new line character is read always
    if bytes > 1 {
        p = PathBuf::from(&input);
    }

    //Write file to disk
    let mut file = File::create(&p)?;
    let data = toml::to_string(spec)?;
    let _res = write!(file, "{}", data)?;

    Ok(true)
}

fn command_add_nodes(node_type : String, num : usize, spec : &mut TestSpec, data : &TestBasics) -> Result<bool, Errors> {
    let mut rng = rand::thread_rng();
    let sample_limits = Uniform::new(0.0, data.side_size + 1.0);

    let ref mut nodes = match node_type.to_uppercase().as_str() {
        "AVAILABLE" => &mut spec.available_nodes,
        "INITIAL" => &mut spec.initial_nodes,
        &_ => return Err(Errors::TestParsing(format!("{} is not a supported type of node", &node_type)))
    };

    //Add the nodes
    for i in 1..num+1 {
        let mut w = WorkerConfig::new();
        w.worker_name = format!("{}{}", DEFAULT_NODE_NAME, i);
        w.operation_mode = worker::OperationMode::Simulated; //All test files are for simulated mode.
        w.random_seed = rng.next_u32();
        w.work_dir = data.work_dir.clone();
        w.protocol = data.protocol.clone();
        
        //Calculate the position
        let x = rng.sample(sample_limits);
        let y = rng.sample(sample_limits);
        w.position = Position{ x : x, y : y};

        //Create the radio configurations
        let mut sr = RadioConfig::new();
        sr.range = DEFAULT_SHORT_RADIO_RANGE;
        w.radio_short = Some(sr);

        let mut lr = RadioConfig::new();
        lr.range = DEFAULT_LONG_RADIO_RANGE;
        lr.interface_name = Some(format!("{}1", DEFAULT_INTERFACE_NAME));
        w.radio_long = Some(lr);
        
        //Add the configuration to the nodes
        nodes.insert(w.worker_name.clone(), w);
    }

    //println!("Nodes: {:?}", &nodes);

    Ok(false)
}

fn capture_basic_data() -> Result<TestBasics, Errors> {
    let mut rng = thread_rng();
    let mut input = String::new();
    let mut data = TestBasics::new();

    println!("Welcome to testgen! \nWe'll be creating a new test specification file to use with master_cli from the MeshSim suite of tools.");
    println!("Let's start capturing the the basic data for the test.");
    println!("Name for the test [{}]: ", DEFAULT_FILE_NAME);
    let bytes_read = io::stdin().read_line(&mut input)?;
    if bytes_read > 1 {
        data.test_name = input[0..input.len()-1].into();
    } else {
        data.test_name = DEFAULT_FILE_NAME.into();
    }
    input.clear();

    println!("Input the duration for the test in milliseconds: ");
    let bytes_read = io::stdin().read_line(&mut input)?;
    data.end_time = match input[0..input.len()-1].parse() {
        Ok(t) => t,
        Err(e) => return Err(Errors::TestParsing(format!("{}", e)))
    };
    input.clear();

    println!("What protocol will the workers be running? ");
    let bytes_read = io::stdin().read_line(&mut input)?;
    let prot_input : String = input[0..input.len()-1].into();
    data.protocol = match prot_input.parse::<Protocols>() {
        Ok(p) => p,
        Err(e) => return Err(Errors::TestParsing(format!("{}", e)))
    };
    input.clear();

    println!("What size (in meters) should the side of the area be? (The Simulation area is square-shaped)");
    let bytes_read = io::stdin().read_line(&mut input)?;
    data.side_size = match input[0..input.len()-1].parse() {
        Ok(t) => t,
        Err(e) => return Err(Errors::TestParsing(format!("{}", e)))
    };
    input.clear();

    let default_work_dir = format!("/tmp/{}{}", &data.test_name, rng.next_u32());
    println!("Input the working directory for the test [{}] ", &default_work_dir);
    let bytes_read = io::stdin().read_line(&mut input)?;
    if bytes_read > 1 {
        data.work_dir = input[0..input.len()-1].into();
    } else {
        data.work_dir = default_work_dir;
    }
    input.clear();

    Ok(data)
}

fn main() {
    let data = match capture_basic_data() {
        Ok(d) => d,
        Err(e) => {
            println!("testgen failed with the following error: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(ref e) = start_command_loop(data) {
        println!("testgen failed with the following error: {}", e);
        std::process::exit(1);
    }
}