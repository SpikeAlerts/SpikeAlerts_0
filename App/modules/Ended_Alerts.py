### Import Packages

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 

from App.modules import Basic_PSQL as psql
from App.modules import Our_Queries as query
import psycopg2
from psycopg2 import sql

# Data Manipulation

import pandas as pd

# Messages

from App.modules import Create_messages

## Workflow

def workflow(sensors_dict, purpleAir_runtime, messages, record_ids_to_text, reports_for_day, base_report_url, can_text, pg_connection_dict):
    '''
    Runs the full workflow for a new spike
    
    Needs new_spikes_df (pd.DataFrame with columns 'sensor_index' int  and 'pm25' float, 
            purpleAir_runtime (datetime timestamp))
    '''
    
    if len(sensors_dict['ended']) > 0:
    
    # 1) Add alert to archive
        Add_to_archived_alerts(sensors_dict['not'], pg_connection_dict)

        # 2) Remove from Active Alerts
        
        ended_alert_indices = Remove_active_alerts(sensors_dict['not'], pg_connection_dict) # # A list

        # 3) Transfer these alerts from "Sign Up Information" active_alerts to "Sign Up Information" cached_alerts 
        
        Cache_alerts(ended_alert_indices, pg_connection_dict)
        
    else:
        ended_alert_indices = []
        
    # 4) Query for people to text about ended alerts (subscribed = TRUE and active_alerts is empty and cached_alerts not empty and cached_alerts 
        
    record_ids_end_alert_message = query.Get_users_to_message_end_alert(pg_connection_dict, ended_alert_indices)
            
    # 5) If #4 has elements: for each element (user) in #4
    
    if len(record_ids_end_alert_message) > 0:
    
        for record_id in record_ids_end_alert_message:
            
            # a) Initialize report - generate unique report_id, log cached_alerts and use to find start_time/max reading/duration/sensor_indices
                
            duration_minutes, max_reading, report_id = Initialize_report(record_id, reports_for_day, pg_connection_dict)
            
            reports_for_day += 1 
            
            if can_text: # Waking hours

                # b) Compose message telling user it's over w/ unique report option & concat to messages/record_ids_to_text
                
                record_ids_to_text += [record_id]
                messages += [Create_messages.end_alert_message(duration_minutes, max_reading, report_id, base_report_url)] # in Create_messages.py

        # c) Clear the users' cached_alerts 
        
        Clear_cached_alerts(record_ids_end_alert_message, pg_connection_dict)
    
    return messages, record_ids_to_text, reports_for_day
    
# ~~~~~~~~~~~~~~~~~~~~~ 
    
 # For each ENDED alert, we should

# 1) Add to archived alerts

def Add_to_archived_alerts(not_spiked_sensors, pg_connection_dict):
    '''
    not_spiked_sensors is a set of sensor indices that have ended spikes Alerts
    '''

    # Get relevant sensor indices as list
    sensor_indices = list(not_spiked_sensors)

    # This command selects the ended alerts from active alerts
    # Then it gets the difference from the current time and when it started
    # Lastly, it inserts this selection while converting that time difference into minutes for duration_minutes column
    cmd = sql.SQL('''
    WITH ended_alerts as
    (
SELECT alert_index, sensor_indices, start_time, CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago' - start_time as time_diff, max_reading 
FROM "Active Alerts Acute PurpleAir"
WHERE sensor_indices <@ {}::int[] -- contained
    )
    INSERT INTO "Archived Alerts Acute PurpleAir" 
    SELECT alert_index, sensor_indices, start_time, (((DATE_PART('day', time_diff) * 24) + 
    DATE_PART('hour', time_diff)) * 60 + DATE_PART('minute', time_diff)) as duration_minutes, max_reading
    FROM ended_alerts;
    ''').format(sql.Literal(sensor_indices))
    
    psql.send_update(cmd, pg_connection_dict)
    

#~~~~~~~~~~~~~~~~

# 2) Remove from active alerts

def Remove_active_alerts(not_spiked_sensors, pg_connection_dict):
    '''
    This function removes the ended_spikes from the Active Alerts Table
    It also retrieves their alert_index
    
    ended_spike_sensors is a set of sensor indices that have ended spikes Alerts
    
    ended_alert_indices is returned alert_indices (as a list) of the removed alerts for accessing Archive for end message 
    
    '''

    # Get relevant sensor indices as list
    sensor_indices = list(not_spiked_sensors)
    
    cmd = sql.SQL('''
    SELECT alert_index
    FROM "Active Alerts Acute PurpleAir"
    WHERE sensor_indices <@ {}::int[]; -- contained;
    ''').format(sql.Literal(sensor_indices))
    
    response = psql.get_response(cmd, pg_connection_dict)
    
    ended_alert_indices = [i[0] for i in response]
    
    cmd = sql.SQL('''
    DELETE FROM "Active Alerts Acute PurpleAir"
    WHERE sensor_indices <@ {}::int[]; -- contained;
    ''').format(sql.Literal(sensor_indices))
    
    psql.send_update(cmd, pg_connection_dict)  
    
    return ended_alert_indices

# ~~~~~~~~~~~~~~ 
def Initialize_report(record_id, reports_for_day, pg_connection_dict):
    '''
    This function will initialize a unique report for a user in the database.

    It will also return the duration_minutes/max_reading/report_id of the report
    '''
    
    # Create Report_id
    
    report_date = dt.datetime.now(pytz.timezone('America/Chicago')).replace(minute=0, second=1) - dt.timedelta(hours=8) # Making sure date aligns with daily update (8am)
    
    report_id = str(reports_for_day).zfill(5) + '-' + report_date.strftime('%m%d%y') # XXXXX-MMDDYY
    # Use the record_id to query for the user's cached_alerts
    # Then aggregate from those alerts the start_time, time_difference, max_reading, and nested sensor_indices
    # Unnest the sensor indices into an array of unique sensor_indices
    # Lastly, it will insert all the information into "Reports Archive"
    
    cmd = sql.SQL('''WITH alert_cache as
(
	SELECT cached_alerts
	FROM "Sign Up Information"
	WHERE record_id = {} --inserted record_id
), alerts as
(
	SELECT MIN(p.start_time) as start_time,
			CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago' 
				- MIN(p.start_time) as time_diff,
			MAX(p.max_reading) as max_reading, 
			ARRAY_AGG(p.sensor_indices) as sensor_indices
	FROM "Archived Alerts Acute PurpleAir" p, alert_cache c
	WHERE p.alert_index = ANY (c.cached_alerts)
), unnested_sensors as 
(
	SELECT ARRAY_AGG(DISTINCT x.v) as sensor_indices
	FROM alerts cross JOIN LATERAL unnest(alerts.sensor_indices) as x(v)
)
INSERT INTO "Reports Archive"
SELECT {}, -- Inserted report_id
        a.start_time, -- start_time
		(((DATE_PART('day', a.time_diff) * 24) + 
    		DATE_PART('hour', a.time_diff)) * 60 + 
		 	DATE_PART('minute', a.time_diff)) as duration_minutes,
			a.max_reading, -- max_reading
		n.sensor_indices,
		c.cached_alerts
FROM alert_cache c, alerts a, unnested_sensors n;
''').format(sql.Literal(record_id),
            sql.Literal(report_id))

    psql.send_update(cmd, pg_connection_dict)

    # Now get the information from that report

    cmd = sql.SQL('''SELECT duration_minutes, max_reading
             FROM "Reports Archive"
             WHERE report_id = {};
''').format(sql.Literal(report_id))

    response = psql.get_response(cmd, pg_connection_dict)

    # Unpack response
    duration_minutes, max_reading = response[0]

    return duration_minutes, max_reading, report_id    
# ~~~~~~~~~~~~~~~~

# 3) Transfer these alerts from "Sign Up Information" active_alerts to "Sign Up Information" cached_alerts

def Cache_alerts(ended_alert_indices, pg_connection_dict):
    '''
    This function transfers a list of ended_alert_indices from "Sign Up Information" active_alerts to "Sign Up Information" cached_alerts
    '''
    
    # Create Cursor for commands
    conn = psycopg2.connect(**pg_connection_dict,
                            keepalives_idle=50)
    cur = conn.cursor()
    
    for alert_index in ended_alert_indices:
    
        cmd = sql.SQL('''
        UPDATE "Sign Up Information"
        SET active_alerts = ARRAY_REMOVE(active_alerts, {}), -- Inserted alert_index
            cached_alerts = ARRAY_APPEND(cached_alerts, {}) -- Inserted alert_index
        WHERE {} = ANY (active_alerts);
        ''').format(sql.Literal(alert_index),
                    sql.Literal(alert_index),
                    sql.Literal(alert_index)
                   )
        cur.execute(cmd)
    # Commit command
    conn.commit()
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
# ~~~~~~~~~~~~~~~~
    
# 5c) Clear a users' cache

def Clear_cached_alerts(record_ids, pg_connection_dict):
    '''
    This function clears the cached_alerts for all users with the given record_ids (a list of integers)
    '''
    
    cmd = sql.SQL('''
    UPDATE "Sign Up Information"
    SET cached_alerts = {} 
    WHERE record_id = ANY ( {} );
    ''').format(sql.Literal('{}'),
                sql.Literal(record_ids)
               )
    
    psql.send_update(cmd, pg_connection_dict)       
