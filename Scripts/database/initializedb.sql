CREATE table "Sign Up Information"
	(user_index serial, first_name text, last_name text,
	 intersection_index int, phone_number int, email text, 
	 opt_in_phone boolean, opt_in_email boolean, last_messaged timestamp);

CREATE table "Active Alerts Acute PurpleAir"
	(alert_index bigserial, sensor_index int, start_time timestamp, max_reading float);

CREATE table "Archived Alerts Acute PurpleAir"
(alert_index bigint, sensor_idex int, start_time timestamp, duration_minutes integer, max_reading float);


