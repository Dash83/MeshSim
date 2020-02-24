-- Your SQL goes here
CREATE TABLE workers (
    id serial PRIMARY KEY,
    Worker_Name varchar (100) NOT NULL,
    Worker_ID varchar (100) NOT NULL,
    Short_Range_Address varchar(50),
    Long_Range_Address varchar(50)
);

CREATE TABLE worker_positions (
    worker_id integer REFERENCES workers(id) PRIMARY KEY,
    x real NOT NULL,
    y real NOT NULL
);

CREATE TABLE worker_velocities (
    worker_id integer REFERENCES workers(id) PRIMARY KEY,
    vel_x REAL NOT NULL,
    vel_y REAL NOT NULL
);

CREATE TABLE worker_destinations (
    worker_id integer REFERENCES workers(id) PRIMARY KEY,
    dest_x real NOT NULL,
    dest_y real NOT NULL
);

CREATE TABLE active_wifi_transmitters (
    worker_id integer REFERENCES workers(id) PRIMARY KEY
);

CREATE TABLE active_lora_transmitters (
    worker_id integer REFERENCES workers(id) PRIMARY KEY
);