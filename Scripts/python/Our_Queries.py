# Queries for our database

## Load modules

from psycopg2 import sql
import Basic_PSQL as psql
import pandas as pd


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# GetSort_Spikes

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Function to get the sensor_ids from our database

def Get_sensor_ids(pg_connection_dict):
    '''
    This function gets the sensor_ids of all sensors in our database that are not flagged from previous days.
    Returns a list of integers
    '''

    cmd = sql.SQL('''SELECT sensor_index 
    FROM "PurpleAir Stations"
    WHERE channel_flags = ANY (ARRAY[0,4]) AND channel_state = 3; -- channel_flags are updated daily, channel_state <- managed by someone - 3 = on, 0 = off
    ''')
    
    response = psql.get_response(cmd, pg_connection_dict)

    # Unpack response into pandas series

    sensor_ids = [int(i[0]) for i in response]

    return sensor_ids

# ~~~~~~~~~~~~~~~~~~
    
def Get_previous_active_sensors(pg_connection_dict):
    '''
    Get active alerts from database sensor_indices.
    Returns active_alerts pd.DataFrame with a column of sensor_indices (lists are the elements)
    '''
    
    cmd = sql.SQL('''SELECT sensor_indices 
    FROM "Active Alerts Acute PurpleAir"
    ''')
    
    response = psql.get_response(cmd, pg_connection_dict)   
    # Convert response into dataframe
    
    cols_for_active_alerts = ['sensor_indices']
    active_alerts_df = pd.DataFrame(response, columns = cols_for_active_alerts)

    
    return active_alerts_df

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~p.last_elevated + INTERVAL '30 Minutes' > CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago'

def Get_not_elevated_sensors(pg_connection_dict, alert_lag=20):
    '''
    Get sensor_indices from database where the sensor has not been elevated in 30 minutes
    Returns sensor_indices
    '''
    
    cmd = sql.SQL(f'''SELECT sensor_index 
    FROM "PurpleAir Stations"
    WHERE last_elevated + INTERVAL '{alert_lag} Minutes' < CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago';
    ''')
    
    response = psql.get_response(cmd, pg_connection_dict)   
    # Convert response into dataframe
    
    sensor_indices = [i[0] for i in response] # Unpack results into list

    return sensor_indices
#~~~~~~~~~~~~~~~~~
# New_Alerts

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_active_users_nearby_sensor(pg_connection_dict, sensor_index, distance=1000):
    '''
    This function will return a list of record_ids from "Sign Up Information" that are within the distance from the sensor and subscribed
    
    sensor_index = integer
    distance = integer (in meters)
    
    returns record_ids (a list)
    '''

    cmd = sql.SQL('''
    WITH sensor as -- query for the desired sensor
    (
    SELECT sensor_index, geometry
    FROM "PurpleAir Stations"
    WHERE sensor_index = {}
    )
    SELECT record_id
    FROM "Sign Up Information" u, sensor s
    WHERE u.subscribed = TRUE AND ST_DWithin(ST_Transform(u.geometry,26915), -- query for users within the distance from the sensor
										    ST_Transform(s.geometry, 26915),{}); 
    ''').format(sql.Literal(sensor_index),
                sql.Literal(distance))

    response = psql.get_response(cmd, pg_connection_dict)

    record_ids = [i[0] for i in response] # Unpack results into list

    return record_ids

# ~~~~~~~~~~~~~~


def Get_users_to_message_new_alert(pg_connection_dict, record_ids):
    '''
    This function will return a list of record_ids from "Sign Up Information" that have empty active and cached alerts and are in the list or record_ids given
    
    record_ids = a list of ids to check
    
    returns record_ids_to_text (a list)
    '''

    cmd = sql.SQL('''
    SELECT record_id
    FROM "Sign Up Information"
    WHERE active_alerts = {} AND cached_alerts = {} AND record_id = ANY ( {} );
    ''').format(sql.Literal('{}'), sql.Literal('{}'), sql.Literal(record_ids))

    response = psql.get_response(cmd, pg_connection_dict)

    record_ids_to_text = [i[0] for i in response]

    return record_ids_to_text
    
# ~~~~~~~~~~~~~~~~~~~~~~~~

def Get_users_to_message_end_alert(pg_connection_dict, ended_alert_indices):
    '''
    This function will return a list of record_ids from "Sign Up Information" that are subscribed, have empty active_alerts, non-empty cached_alerts
    
    ended_alert_indices = a list of alert_ids that just ended
    
    returns record_ids_to_text (a list)
    '''

    cmd = sql.SQL('''
    SELECT record_id
    FROM "Sign Up Information"
    WHERE subscribed = TRUE
        AND active_alerts = {}
    	AND ARRAY_LENGTH(cached_alerts, 1) > 0;
    ''').format(sql.Literal('{}'))

    response = psql.get_response(cmd, pg_connection_dict)

    record_ids_to_text = [i[0] for i in response]

    return record_ids_to_text
    
# ~~~~~~~~~~~~~~ 


