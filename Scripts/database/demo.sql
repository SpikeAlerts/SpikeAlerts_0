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

UPDATE "PurpleAir Stations"
SET channel_state = 0
WHERE sensor_index = 145502;
