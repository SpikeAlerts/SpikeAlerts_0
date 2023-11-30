-- VIEW? Find users' last_elevated (CTE)

WITH users as -- Select users to potentially message
(
SELECT record_id, geometry
FROM "Sign Up Information"
WHERE record_id = ANY ( ARRAY[1,2,3,4,5] ) -- inserted record_ids
)
SELECT u.record_id, MAX(p.last_elevated) as last_elevated
FROM users u, "PurpleAir Stations" p
WHERE ST_DWithin(u.geometry, p.geometry, 1000) -- All Sensors within 1 kilometer
GROUP BY u.record_id;

-- VIEW? Find alerts' last_seen (CTE) <- Could be changed to last_elevated

WITH alerts as -- Find alerts to potentially activate again
(
SELECT alert_index, sensor_indices
FROM "Archived Alerts Acute PurpleAir"
WHERE start_time + INTERVAL '1 Minutes' * duration_minutes > 
			CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago' - INTERVAL '30 Minutes' -- Alerts ended within 30 minutes
)
SELECT a.alert_index, MAX(p.last_seen) as last_seen
FROM alerts a, "PurpleAir Stations" p
WHERE p.sensor_index = ANY (a.sensor_indices) -- All Sensors within the sensor_indices
GROUP BY a.alert_index;

-- Show coverage (1km buffer, can change 1000 to 1609 for 1 mile)

with buff as
	(
	SELECT ST_Transform(
			ST_Buffer(
			ST_Transform(s.geometry,26915),
					1000),
					4326) as geom 
	FROM "PurpleAir Stations" s
	WHERE s.channel_state = 3 AND s.channel_flags = 0
	)
SELECT ST_UNION(geom) as coverage
FROM buff;

-- Sample users for export (only for testing purposes!)
select record_id, ST_asText(geometry)
from "Sign Up Information";

-- Look at all the tables

select *
from "Sign Up Information";

select *
from "PurpleAir Stations";

select *
from "PurpleAir Stations"
WHERE channel_state = 3;

select *
from "Active Alerts Acute PurpleAir";

select *
from "Archived Alerts Acute PurpleAir";

select *
from "Reports Archive";

-- For reporting to the City?

with alerts as 
(
	SELECT alert_index, sensor_indices[1] as sensor_index, start_time, duration_minutes, max_reading
	FROM "Archived Alerts Acute PurpleAir"
	WHERE start_time > DATE('2023-11-17')
)
SELECT a.alert_index,
		p.name, 
		a.sensor_index,
		a.start_time, 
		a.duration_minutes, 
		a.max_reading,
		ST_X(p.geometry) as longitude, 
		ST_Y(p.geometry) as latitude
FROM alerts a
INNER JOIN "PurpleAir Stations" p ON (p.sensor_index = a.sensor_index);

-- This shows all the active alerts at each station

WITH alerts as
(
	SELECT start_time, max_reading, 
			sensor_indices[1] as sensor_index
	FROM "Active Alerts Acute PurpleAir"
	GROUP BY sensor_indices[1]
)
SELECT a.sensor_index, a.start_time, a.max_reading, p.geometry
FROM alerts a
INNER JOIN "PurpleAir Stations" p ON (p.sensor_index = a.sensor_index);

-- This shows a summary of the alerts at each station beyond a specified date

WITH alerts as
(
	SELECT COUNT(start_time) as count, ARRAY_AGG(start_time) as start_time, 
			AVG(duration_minutes) as duration_minutes, 
			AVG(max_reading) as max_reading, 
			sensor_indices[1] as sensor_index
	FROM "Archived Alerts Acute PurpleAir"
	WHERE start_time > DATE('2023-11-25')
	GROUP BY sensor_indices[1]
)
SELECT a.sensor_index, a.count, a.start_time, a.duration_minutes, a.max_reading, p.geometry
FROM alerts a
INNER JOIN "PurpleAir Stations" p ON (p.sensor_index = a.sensor_index);

-- Find users within a distance from a sensor (1km is used, but can change 1000 to 1609.34)

WITH sensor as -- query for the desired sensor
    (
    SELECT sensor_index, geometry
    FROM "PurpleAir Stations"
    WHERE sensor_index = 156605
    )
    SELECT *
    FROM "Sign Up Information" u, sensor s
    WHERE u.subscribed = TRUE AND ST_DWithin(ST_Transform(u.geometry,26915), -- query for users within the distance from the sensor
										    ST_Transform(s.geometry, 26915),1000); --1609.34 meters in a mile
