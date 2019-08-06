extern crate mesh_simulator;

use mesh_simulator::master::test_specification::*;
use mesh_simulator::master::MobilityModels;
use mesh_simulator::worker;
use mesh_simulator::worker::mobility::*;
use mesh_simulator::worker::protocols::Protocols;
use mesh_simulator::worker::worker_config::*;

use rand::distributions::{Normal, Uniform};
use rand::{thread_rng, Rng, RngCore};
use std::error;
use std::fmt;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;

const DEFAULT_FILE_NAME: &str = "untitled";
const DEFAULT_NODE_NAME: &str = "node";

#[derive(Debug)]
struct TestBasics {
    pub test_name: String,
    pub end_time: u64,
    pub protocol: Protocols,
    pub width: f64,
    pub height: f64,
    pub m_model: Option<MobilityModels>,
    pub work_dir: String,
}

impl TestBasics {
    pub fn new() -> TestBasics {
        TestBasics {
            test_name: String::from(""),
            end_time: 0,
            protocol: Protocols::NaiveRouting,
            width: 0.0,
            height: 0.0,
            m_model: None,
            work_dir: String::from(""),
        }
    }
}
#[derive(Debug)]
enum Commands {
    ///Finish this configuration and write test file to disk
    Finish,
    ///Add nodes to the test. Params: Type of nodes [Initial/Available], Number of nodes.
    AddNodes(String, usize),
    ///Add a test action to the configuration
    AddAction(String),
    ///Adds a given number of sources
    /// Params: Number of sources, Type of Source, other params (dependant on the source type)
    AddSources(usize, String, String),
    ///Add a grid-arrangement of nodes of size X * Y. The separation of the nodes depends on the
    ///radio ranged passed.
    /// Params:
    /// X - number of columns.
    /// Y - number of rows.
    /// Range - radio range used for node placement.
    AddGrid(usize, usize, f64, f64),
}

impl FromStr for Commands {
    type Err = Errors;

    fn from_str(s: &str) -> Result<Commands, Errors> {
        let parts: Vec<&str> = s.split_whitespace().collect();

        //Assuming here we can't have actions with 0 parameters.
        if !parts.is_empty() {
            match parts[0].to_uppercase().as_str() {
                "FINISH" => Ok(Commands::Finish),
                "ADD_NODES" => {
                    if parts.len() < 3 {
                        //Error out
                        return Err(Errors::TestParsing(String::from(
                            "Add_nodes requires 2 params: node_type [Initial/Available] and number",
                        )));
                    }
                    let node_type = parts[1].into();
                    let number: usize = match parts[2].parse() {
                        Ok(n) => n,
                        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
                    };

                    Ok(Commands::AddNodes(node_type, number))
                }
                "ADD_ACTION" => {
                    if parts.len() < 2 {
                        //Error out
                        return Err(Errors::TestParsing(String::from("No action was specified")));
                    }
                    let mut action: String = String::new();
                    for s in parts[1..].iter() {
                        action.push_str(&format!("{} ", s));
                    }

                    Ok(Commands::AddAction(action))
                }
                "ADD_SOURCES" => {
                    if parts.len() < 3 {
                        //Error out
                        return Err(Errors::TestParsing(String::from(
                            "Add_Sources requires NUM_SOURCES TYPE params",
                        )));
                    }
                    let num: usize = match parts[1].parse() {
                        Ok(n) => n,
                        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
                    };
                    let source_type: String = parts[2].into();
                    let params: String = parts[3..].join(" ");

                    Ok(Commands::AddSources(num, source_type, params))
                }
                "ADD_GRID" => {
                    if parts.len() < 4 {
                        //Error out
                        return Err(Errors::TestParsing(String::from(
                            "Add_Grid requires X, Y RANGE params",
                        )));
                    }
                    let x: usize = match parts[1].parse() {
                        Ok(n) => n,
                        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
                    };
                    let y: usize = match parts[2].parse() {
                        Ok(n) => n,
                        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
                    };
                    let sr_range: f64 = match parts[3].parse() {
                        Ok(n) => n,
                        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
                    };
                    let lr_range: f64 = match parts[4].parse() {
                        Ok(n) => n,
                        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
                    };
                    Ok(Commands::AddGrid(x, y, sr_range, lr_range))
                }
                _ => Err(Errors::TestParsing(format!(
                    "Unsupported command: {:?}",
                    parts
                ))),
            }
        } else {
            //Error out
            Err(Errors::TestParsing(format!(
                "Unsupported command: {:?}",
                parts
            )))
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

    fn cause(&self) -> Option<&dyn error::Error> {
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
            Errors::TestParsing(ref err_str) => {
                write!(f, "Error creating test specification: {}", err_str)
            }
            Errors::IO(ref err) => write!(f, "IO error: {}", err),
            Errors::Serialization(ref err) => write!(f, "Serialization error: {}", err),
        }
    }
}

//Error conversions
impl From<io::Error> for Errors {
    fn from(err: io::Error) -> Errors {
        Errors::IO(err)
    }
}

//Error conversions
impl From<toml::ser::Error> for Errors {
    fn from(err: toml::ser::Error) -> Errors {
        Errors::Serialization(err)
    }
}
//endregion

fn start_command_loop(data: TestBasics) -> Result<(), Errors> {
    let mut input = String::new();
    let mut spec = TestSpec::new();
    let mut stdout = io::stdout();

    //Copy all the basic data to the spec before starting to take commands
    //Get the test measurements
    spec.area_size = Area {
        width: data.width,
        height: data.height,
    };
    //Get test duration
    spec.duration = data.end_time;
    //Get mobility model
    spec.mobility_model = data.m_model.clone();

    println!("Input commands to continue building the test spec.");
    println!("Input ? for a list of commands or \"finish\" for writing the current configuration to file.");
    loop {
        print!("> ");
        stdout.flush()?;
        match io::stdin().read_line(&mut input) {
            Ok(_bytes) => match input.parse::<Commands>() {
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
                }
                Err(e) => {
                    println!("Error parsing command: {}", e);
                }
            },
            Err(error) => {
                println!("{}", error);
            }
        }
        input.clear();
    }
    Ok(())
}

fn process_command(com: Commands, spec: &mut TestSpec, data: &TestBasics) -> Result<bool, Errors> {
    match com {
        Commands::Finish => command_finish(spec, data),
        Commands::AddNodes(node_type, num) => command_add_nodes(node_type, num, spec, data),
        Commands::AddAction(parts) => command_add_action(parts, spec),
        Commands::AddSources(num_sources, s_type, params) => {
            command_add_sources(num_sources, s_type, params, spec)
        }
        Commands::AddGrid(x, y, sr_range, lr_range) => {
            command_add_grid(x, y, sr_range, lr_range, spec, &data)
        }
    }
}

fn command_finish(spec: &mut TestSpec, data: &TestBasics) -> Result<bool, Errors> {
    let mut p = PathBuf::from(".");
    //Determine file name to write
    spec.name = data.test_name.clone();
    let file_name = {
        if spec.name.eq("") {
            format!("{}.toml", DEFAULT_FILE_NAME)
        } else {
            format!("{}.toml", &spec.name)
        }
    };
    p.push(file_name);
    //let canon = p.canonicalize().expect("Invalid file path");

    let mut input = String::new();
    print_test_status(&spec);
    println!(
        "Writing {:?} to disk. Press enter to confirm, or provide a new path.",
        &p
    );
    let bytes = io::stdin().read_line(&mut input)?;
    //new line character is read always
    if bytes > 1 {
        p = PathBuf::from(&input);
    }

    //Write file to disk
    let mut file = File::create(&p)?;
    let data = toml::to_string(spec)?;
    write!(file, "{}", data)?;

    Ok(true)
}

fn command_add_nodes(
    node_type: String,
    num: usize,
    spec: &mut TestSpec,
    data: &TestBasics,
) -> Result<bool, Errors> {
    let mut rng = rand::thread_rng();
    let width_sample = Uniform::new(0.0, data.width);
    let height_sample = Uniform::new(0.0, data.height);
    let walking_sample = Normal::new(HUMAN_SPEED_MEAN, HUMAN_SPEED_STD_DEV);

    let nodes = match node_type.to_uppercase().as_str() {
        "AVAILABLE" => &mut spec.available_nodes,
        "INITIAL" => &mut spec.initial_nodes,
        &_ => {
            return Err(Errors::TestParsing(format!(
                "{} is not a supported type of node",
                &node_type
            )))
        }
    };

    //Add the nodes
    for i in 1..=num {
        let mut w = WorkerConfig::new();
        w.worker_name = format!("{}{}", DEFAULT_NODE_NAME, i);
        w.operation_mode = worker::OperationMode::Simulated; //All test files are for simulated mode.
        w.random_seed = rng.next_u32();
        w.work_dir = data.work_dir.clone();
        w.protocol = data.protocol;
        w.worker_id = Some(WorkerConfig::gen_id(w.random_seed));

        //Calculate the position
        let x = rng.sample(width_sample);
        let y = rng.sample(height_sample);
        w.position = Position { x, y };

        if let Some(model) = &data.m_model {
            match model {
                MobilityModels::RandomWaypoint => {
                    let target_x = rng.sample(width_sample);
                    let target_y = rng.sample(height_sample);
                    w.destination = Some(Position {
                        x: target_x,
                        y: target_y,
                    });

                    //Velocity vector should point to destination
                    let vel = rng.sample(walking_sample);
                    let distance: f64 =
                        euclidean_distance(w.position.x, w.position.y, target_x, target_y);
                    let time: f64 = distance / vel;
                    let x_vel = (target_x - w.position.x) / time;
                    let y_vel = (target_y - w.position.y) / time;
                    w.velocity = Velocity { x: x_vel, y: y_vel };
                }
                MobilityModels::Stationary => { /* Nothing else to do */ }
            }
        } else {
            //Calculate velocity
            // let x_vel = rng.sample(walking_sample);
            // let y_vel = rng.sample(walking_sample);
            //Since no mobility model is provided, assume velocity of 0
            let x_vel = 0.0f64;
            let y_vel = 0.0f64;
            w.velocity = Velocity { x: x_vel, y: y_vel };
        }

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

fn command_add_action(parts: String, spec: &mut TestSpec) -> Result<bool, Errors> {
    spec.actions.push(parts);
    Ok(false)
}

fn command_add_sources(
    num_sources: usize,
    source_type: String,
    params: String,
    spec: &mut TestSpec,
) -> Result<bool, Errors> {
    eprintln!("num_sources: {}", num_sources);
    eprintln!("initial_nodes: {}", spec.initial_nodes.len());
    if num_sources > spec.initial_nodes.len() || spec.initial_nodes.len() < 2 {
        return Err(Errors::TestParsing(format!(
            "Not enough nodes in the spec to support {} sources",
            num_sources
        )));
    }

    match source_type.to_uppercase().as_str() {
        "CBR" => add_cbr_sources(num_sources, params, spec),
        _ => Err(Errors::TestParsing(format!(
            "Unsupported source type: {}",
            &source_type
        ))),
    }
}

fn add_cbr_sources(
    num_sources: usize,
    params: String,
    spec: &mut TestSpec,
) -> Result<bool, Errors> {
    let mut rng = thread_rng();
    //Earliest start time for any CBR source is when 10% of the sim time has started.
    let sources_start: u64 = spec.duration / 10;
    let start_times_sample = Uniform::new(sources_start, spec.duration);

    let param_parts: Vec<&str> = params.split_whitespace().collect();
    //get packet-rate
    let packet_rate: usize = match param_parts[0].parse() {
        Ok(n) => n,
        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
    };
    //select packet-size
    let packet_size: usize = match param_parts[1].parse() {
        Ok(n) => n,
        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
    };

    for _i in 0..num_sources {
        //select source
        let source_index = rng.next_u32() as usize % spec.initial_nodes.len();
        let source_name = spec.initial_nodes.keys().nth(source_index).unwrap();
        //select destination
        let mut dest_index = rng.next_u32() as usize % spec.initial_nodes.len();
        while dest_index == source_index {
            dest_index = rng.next_u32() as usize % spec.initial_nodes.len();
        }
        let dest_name = spec.initial_nodes.keys().nth(dest_index).unwrap();

        //select start_time
        let start_time = rng.sample(start_times_sample);

        //calculate duration of source
        let duration_mean: f64 = (spec.duration - start_time) as f64 / 2.0;
        let duration_std_dev: f64 = duration_mean * 0.20;
        let duration_sample = Normal::new(duration_mean, duration_std_dev);
        let duration: u64 = rng.sample(duration_sample) as u64;

        let action = format!(
            "ADD_SOURCE {} {} {} {} {} {}",
            &source_name, &dest_name, packet_rate, packet_size, duration, start_time
        );
        spec.actions.push(action);
    }
    Ok(false)
}

fn print_test_status(spec: &TestSpec) {
    println!("Test name: {}", spec.name);
    println!("# of initial nodes: {}", spec.initial_nodes.len());
    println!("# of available nodes: {}", spec.available_nodes.len());
    println!("# of test actions: {}", spec.actions.len());
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
        data.test_name = input[0..input.len() - 1].into();
    } else {
        data.test_name = DEFAULT_FILE_NAME.into();
    }
    input.clear();

    println!("Input the duration for the test in milliseconds: ");
    let _bytes_read = io::stdin().read_line(&mut input)?;
    data.end_time = match input[0..input.len() - 1].parse() {
        Ok(t) => t,
        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
    };
    input.clear();

    println!("What protocol will the workers be running? ");
    let _bytes_read = io::stdin().read_line(&mut input)?;
    let prot_input: String = input[0..input.len() - 1].into();
    data.protocol = match prot_input.parse::<Protocols>() {
        Ok(p) => p,
        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
    };
    input.clear();

    println!("Simulation area Width (in meters):");
    let _bytes_read = io::stdin().read_line(&mut input)?;
    data.width = match input[0..input.len() - 1].parse() {
        Ok(t) => t,
        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
    };
    input.clear();

    println!("Simulation area height (in meters):");
    let _bytes_read = io::stdin().read_line(&mut input)?;
    data.height = match input[0..input.len() - 1].parse() {
        Ok(t) => t,
        Err(e) => return Err(Errors::TestParsing(format!("{}", e))),
    };
    input.clear();

    println!("Mobility model:");
    let _bytes_read = io::stdin().read_line(&mut input)?;
    data.m_model = match input[0..input.len() - 1].parse::<MobilityModels>() {
        Ok(t) => Some(t),
        Err(e) => {
            eprintln!("{}. Setting model to None (stationary)", e);
            None
        }
    };
    input.clear();

    let default_work_dir = format!("/tmp/{}{}", &data.test_name, rng.next_u32());
    println!(
        "Input the working directory for the test [{}] ",
        &default_work_dir
    );
    let bytes_read = io::stdin().read_line(&mut input)?;
    if bytes_read > 1 {
        data.work_dir = input[0..input.len() - 1].into();
    } else {
        data.work_dir = default_work_dir;
    }
    input.clear();

    Ok(data)
}

fn command_add_grid(
    num_columns: usize,
    num_rows: usize,
    sr_range: f64,
    lr_range: f64,
    spec: &mut TestSpec,
    data: &TestBasics,
) -> Result<bool, Errors> {
    let start_x: usize = 0;
    let start_y: usize = 0;
    let effective_range = sr_range * 0.90;
    let mut rng = rand::thread_rng();
    let nodes = &mut spec.initial_nodes;
    let mut count = 0;

    for j in start_x..num_rows {
        for i in start_y..num_columns {
            let x = i as f64 * effective_range;
            let y = j as f64 * effective_range;

            let mut w = WorkerConfig::new();
            count += 1;
            w.worker_name = format!("{}{}", DEFAULT_NODE_NAME, count);
            w.operation_mode = worker::OperationMode::Simulated; //All test files are for simulated mode.
            w.random_seed = rng.next_u32();
            w.work_dir = data.work_dir.clone();
            w.protocol = data.protocol;
            w.worker_id = Some(WorkerConfig::gen_id(w.random_seed));
            w.position = Position { x, y };
            w.velocity = Velocity { x: 0.0, y: 0.0 };

            //Create the radio configurations
            let mut sr = RadioConfig::new();
            sr.range = sr_range;
            w.radio_short = Some(sr);

            let mut lr = RadioConfig::new();
            lr.range = lr_range;
            lr.interface_name = Some(format!("{}1", DEFAULT_INTERFACE_NAME));
            w.radio_long = Some(lr);

            //Add the configuration to the nodes
            nodes.insert(w.worker_name.clone(), w);
        }
    }

    Ok(false)
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
