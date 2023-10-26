-- Run this file to initialize the database, schema, and tables for PurpleAir Monitor Readings in Minneapolis
-- You can run this by using a psql command like:
-- psql "host=postgres.cla.umn.edu user=<your_username> password=<your_password> " -f initialize_db.sql

-- To Do beforehand

--CREATE DATABASE "SpikeAlerts"; -- Create the database

--\c "SpikeAlerts"; -- Connect to database This needs a password!

--CREATE EXTENSION postgis; -- Add spatial extensions
--CREATE EXTENSION postgis_topology;

SET timezone TO 'Europe/Berlin';

CREATE table "Sign Up Information"
	(record_id serial, -- Unique Identifier from REDCap
	last_messaged timestamp DEFAULT CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago', -- Last time messaged
	messages_sent int DEFAULT 1, -- Number of messages sent
	active_alerts bigint [] DEFAULT array[]::bigint [], -- List of Active Alerts
	geometry geometry);

CREATE table "Active Alerts Acute PurpleAir"
	(alert_index bigserial, -- Unique identifier for an air quality spike alert
	 sensor_indices int [] DEFAULT array[]::int [], -- List of Sensor Unique Identifiers 
	  start_time timestamp,
	   max_reading float); -- Maximum value registered from all sensors

CREATE table "Archived Alerts Acute PurpleAir" -- Archive of the Above table
    (alert_index bigint,
    sensor_indices int [], -- List of Sensor Unique Identifiers 
    start_time timestamp,
    duration_minutes integer,
    max_reading float);
    
CREATE TABLE "PurpleAir Stations" -- See PurpleAir API - https://api.purpleair.com/
(
	sensor_index int,
	firmware_version varchar(30),
	date_created timestamp,
	last_modified timestamp, 
	last_seen timestamp,
	"name" varchar(100),
	uptime int,
	position_rating int,
	channel_state int,
	channel_flags int,
	altitude int,
	geometry geometry
);

CREATE TABLE "Minneapolis Boundary"
(
    "CTU_ID" int, -- Unique Identifier
    "CTU_NAME" text, -- City/Township Name
    "CTU_CODE" text, -- City/Township Code
    geometry geometry -- Polygon
);


-- Don't need AADT yet

--CREATE TABLE "MNDOT Current AADT Segments" -- Create table to store information on Current AADT segments - https://gisdata.mn.gov/dataset/trans-aadt-traffic-segments
--( 
--    "SEQUENCE_NUMBER" int, -- Unique identifier
--    "ROUTE_LABEL" text,
--    "STREET_NAME" text,
--    "DAILY_FACTOR" text,
--    "SEASONAL_FACTOR" text,
--    "AXLE_FACTOR" text,
--    "CURRENT_YEAR" int,
--    "CURRENT_VOLUME" int,
--    geometry geometry
--);


-- For relating the location to users
CREATE TABLE "Road Intersections" -- Create table to store road intersections in minneapolis
(
    intersection_index serial, -- Unique identifier
    "NS_cross_street" text, 
    "EW_cross_street" text,
--	nearby_sensors int [], -- nearby sensors as an array of integers
    geometry geometry
);
