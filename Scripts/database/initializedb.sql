-- Run this file to initialize the database, schema, and tables for PurpleAir Monitor Readings in Minneapolis
-- You can run this by using a psql command like:
-- psql "host=postgres.cla.umn.edu user=<your_username> password=<your_password> " -f initialize_db.sql

-- To Do beforehand

--CREATE DATABASE "SpikeAlerts"; -- Create the database

--\c "SpikeAlerts"; -- Connect to database This needs a password!

--CREATE EXTENSION postgis; -- Add spatial extensions
--CREATE EXTENSION postgis_topology;

CREATE table "Sign Up Information"
	(user_index serial, first_name text, last_name text,
	 intersection_index int, phone_number bigint, email text, 
	 opt_in_phone boolean, opt_in_email boolean, last_messaged timestamp, active_alerts bigint []);

CREATE table "Active Alerts Acute PurpleAir"
	(alert_index bigserial, sensor_index int, start_time timestamp, max_reading float);

CREATE table "Archived Alerts Acute PurpleAir"
(alert_index bigint, sensor_idex int, start_time timestamp, duration_minutes integer, max_reading float);

/* adding this to this file in the way I did it for record keeping purposes. would obviously be better to make it as a bigint to begin with. - Priya

- Rob Changed the Create Statement 9/2023, leaving for future reference
-- 
-- ALTER TABLE "Sign Up Information"
--   ALTER COLUMN phone_number TYPE bigint;
*/

CREATE TABLE "Minneapolis Boundary"
(
    "CTU_ID" int, -- Unique Identifier
    "CTU_NAME" text, -- City/Township Name
    "CTU_CODE" text, -- City/Township Code
    geometry geometry -- Polygon
);

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

CREATE TABLE "MNDOT Current AADT Segments" -- Create table to store information on Current AADT segments - https://gisdata.mn.gov/dataset/trans-aadt-traffic-segments
( 
    "SEQUENCE_NUMBER" int, -- Unique identifier
    "ROUTE_LABEL" text,
    "STREET_NAME" text,
    "DAILY_FACTOR" text,
    "SEASONAL_FACTOR" text,
    "AXLE_FACTOR" text,
    "CURRENT_YEAR" int,
    "CURRENT_VOLUME" int,
    geometry geometry
);

CREATE TABLE "Road Intersections" -- Create table to store road intersections in minneapolis
(
    intersection_index serial, -- Unique identifier
    "NS_cross_street" text, 
    "EW_cross_street" text,
	nearby_sensors int [], -- nearby sensors as an array of integers
    geometry geometry
);

--CREATE TABLE "Nearby Intersections" -- Not sure if we're going this route, yet. Could be a view?
--(
--    intersection_index integer, -- unique identifier
--    nearby_sensors int [] -- nearby sensors as an array of integers
--    --nearby_facilities int [], -- nearby mpca permitted emitter
--    --nearby_segments int [], -- nearby mndot aadt segment    
--);
