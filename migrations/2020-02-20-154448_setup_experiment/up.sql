-- Your SQL goes here
CREATE TABLE workers (
    id serial PRIMARY KEY,
    worker_name varchar (100) NOT NULL,
    worker_id varchar (100) NOT NULL,
    short_range_address varchar(50),
    long_range_address varchar(50)
);

CREATE UNIQUE INDEX worker_name_idx ON workers (worker_name);

CREATE TABLE worker_positions (
    worker_id integer REFERENCES workers(id) PRIMARY KEY,
    x double precision NOT NULL,
    y double precision NOT NULL
);

CREATE TABLE worker_velocities (
    worker_id integer REFERENCES workers(id) PRIMARY KEY,
    x double precision NOT NULL,
    y double precision NOT NULL
);

CREATE TABLE worker_destinations (
    worker_id integer REFERENCES workers(id) PRIMARY KEY,
    x double precision NOT NULL,
    y double precision NOT NULL
);

CREATE TABLE active_wifi_transmitters (
    worker_id integer REFERENCES workers(id) PRIMARY KEY
);

CREATE TABLE active_lora_transmitters (
    worker_id integer REFERENCES workers(id) PRIMARY KEY
);

CREATE FUNCTION distance(x1 double precision, y1 double precision, x2 double precision, y2 double precision) RETURNS double precision
AS '
	SELECT sqrt((x2 - x1)^2 + (y2 - y1)^2);
' LANGUAGE SQL;