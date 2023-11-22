-- Run this file to initialize the database, schema, and tables for PurpleAir Monitor Readings in Minneapolis
-- You can run this by using a psql command like:
-- psql "host=postgres.cla.umn.edu user=<your_username> password=<your_password> " -f initializedb.sql

-- To Do beforehand

-- CREATE DATABASE "spike_alerts"; -- Create the database

--\c "SpikeAlerts"; -- Connect to database This needs a password!
DROP SCHEMA IF EXISTS spikealerts;
DROP EXTENSION IF EXISTS postgis CASCADE;
DROP EXTENSION IF EXISTS postgis_topology CASCADE;

CREATE SCHEMA spikealerts;
CREATE EXTENSION postgis; -- Add spatial extensions
CREATE EXTENSION postgis_topology;
-- CREATE SCHEMA postgis;


CREATE table spikealerts."Sign Up Information"-- This is our internal record keeping for users
	(record_id integer, -- Unique Identifier from REDCap
	last_messaged timestamp DEFAULT CURRENT_TIMESTAMP, -- Last time messaged
	messages_sent int DEFAULT 1, -- Number of messages sent
	active_alerts bigint [] DEFAULT array[]::bigint [], -- List of Active Alerts
	cached_alerts bigint [] DEFAULT array[]::bigint [], -- List of ended Alerts not yet notified about
	subscribed boolean DEFAULT TRUE, -- Is the user wanting texts? 
	geometry geometry);
	
CREATE INDEX user_gid ON spikealerts."Sign Up Information" USING GIST(geometry);  -- Create spatial index
	
CREATE table spikealerts."Reports Archive"-- These are for reporting to the City and future research
	(report_id varchar(12), -- Unique Identifier with format #####-MMDDYY
	start_time timestamp,
	duration_minutes integer,
	max_reading float, 
	sensor_indices int [], -- List of Sensor Unique Identifiers
	alert_indices bigint [] -- List of Alert Identifiers
    );

CREATE table spikealerts."Active Alerts Acute PurpleAir"
	(alert_index bigserial, -- Unique identifier for an air quality spike alert
	 sensor_indices int [] DEFAULT array[]::int [], -- List of Sensor Unique Identifiers 
	  start_time timestamp,
	   max_reading float); -- Maximum value registered from all sensors

CREATE table spikealerts."Archived Alerts Acute PurpleAir" -- Archive of the Above table
    (alert_index bigint,
    sensor_indices int [], -- List of Sensor Unique Identifiers 
    start_time timestamp,
    duration_minutes integer,
    max_reading float);
    
CREATE TABLE spikealerts."PurpleAir Stations" -- See PurpleAir API - https://api.purpleair.com/
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

CREATE INDEX PurpleAir_gid ON spikealerts."PurpleAir Stations" USING GIST(geometry);  -- Create spatial index for stations

CREATE TABLE spikealerts."Minneapolis Boundary"-- From MN Geocommons - https://gisdata.mn.gov/dataset/us-mn-state-metc-bdry-census2020counties-ctus
(
    "CTU_ID" int, -- Unique Identifier
    "CTU_NAME" text, -- City/Township Name
    "CTU_CODE" text, -- City/Township Code
    geometry geometry -- Polygon
);


-- -- Don't need AADT/intersections

-- --CREATE TABLE "MNDOT Current AADT Segments" -- Create table to store information on Current AADT segments - https://gisdata.mn.gov/dataset/trans-aadt-traffic-segments
-- --( 
-- --    "SEQUENCE_NUMBER" int, -- Unique identifier
-- --    "ROUTE_LABEL" text,
-- --    "STREET_NAME" text,
-- --    "DAILY_FACTOR" text,
-- --    "SEASONAL_FACTOR" text,
-- --    "AXLE_FACTOR" text,
-- --    "CURRENT_YEAR" int,
-- --    "CURRENT_VOLUME" int,
-- --    geometry geometry
-- --);


-- -- For relating the location to users
-- --CREATE TABLE "Road Intersections" -- Create table to store road intersections in minneapolis
-- --(
-- --    intersection_index serial, -- Unique identifier
-- --    "NS_cross_street" text, 
-- --    "EW_cross_street" text,
-- ----	nearby_sensors int [], -- nearby sensors as an array of integers
-- --    geometry geometry
-- --);
