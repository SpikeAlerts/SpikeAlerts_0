-- Show coverage (1km buffer, can change 1000 to 1609 for 1 mile)

with buff as
	(
	SELECT ST_Transform(
			ST_Buffer(
			ST_Transform(s.geometry,26915),
					1000),
					4326) as geom 
	FROM "PurpleAir Stations" s
	WHERE s.channel_state = 3
	)
SELECT ST_UNION(geom) as coverage
FROM buff;

-- Look at all the tables

select *
from "Sign Up Information";

select *
from "PurpleAir Stations";

select *
from "Active Alerts Acute PurpleAir";

select *
from "Archived Alerts Acute PurpleAir";

select *
from "Reports Archive";

-- This shows all the alerts at each station

WITH alerts as
(
	SELECT COUNT(start_time) as count, ARRAY_AGG(start_time) as start_time, 
			ARRAY_AGG(duration_minutes) as duration_minutes, 
			ARRAY_AGG(max_reading) as max_reading, 
			sensor_indices[1] as sensor_index
	FROM "Archived Alerts Acute PurpleAir"
	GROUP BY sensor_indices[1]
)
SELECT a.sensor_index, a.count, a.start_time, a.duration_minutes, a.max_reading, c.geometry
FROM alerts a
INNER JOIN "PurpleAir Stations" c ON (c.sensor_index = a.sensor_index);

-- Find users within a distance (1km is used, but can change 1000 to 1609.34)

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
