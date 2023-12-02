# Queries for our database

## Load modules

from psycopg2 import sql
from App.modules import Basic_PSQL as psql
import pandas as pd
import pytz
import datetime as dt

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Daily_Updates

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~      

def Get_last_PurpleAir_update(pg_connection_dict, timezone = 'America/Chicago'):
    '''
    This function gets the highest last_seen (only updated daily)
    
    returns timezone aware datetime
    '''

    cmd = sql.SQL('''SELECT MAX(last_seen)
    FROM "PurpleAir Stations"
    WHERE channel_flags = 0;
    ''')
    
    response = psql.get_response(cmd, pg_connection_dict)

    # Unpack response into timezone aware datetime
    
    if response[0][0] != None:

        max_last_seen = response[0][0].replace(tzinfo=pytz.timezone(timezone))
    else:
        max_last_seen = dt.datetime(2000, 1, 1).replace(tzinfo=pytz.timezone(timezone))
    
    return max_last_seen
    
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_our_sensor_info(pg_connection_dict):
    '''
    Gets all sensors from our database for a daily update check
    
    returns sensors_df 
    
    with columns sensor_index, last_seen, name, channel_flags, channel_state
    types: int, datetime 'America/Chicago', str, int, int
    '''

    cmd = sql.SQL('''SELECT sensor_index, last_seen, name, channel_flags, channel_state
    FROM "PurpleAir Stations";
    ''')

    response = psql.get_response(cmd, pg_connection_dict)

    # Unpack response into pandas series

    sensors_df = pd.DataFrame(response, columns = ['sensor_index', 'last_seen', 'name', 'channel_flags', 'channel_state'])

    # Datatype corrections
    sensors_df['sensor_index']  = sensors_df.sensor_index.astype(int)
    sensors_df['last_seen'] = pd.to_datetime(sensors_df['last_seen'])
    sensors_df['channel_flags'] = sensors_df.channel_flags.astype(int)
    sensors_df['channel_state'] = sensors_df.channel_state.astype(int)
    
    return sensors_df
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_extent(pg_connection_dict): 
    '''
    Gets the bounding box of our project's extent + 100 meters ("Minneapolis Boundary")
    
    Specifically for PurpleAir api
    
    returns nwlng, selat, selng, nwlat AS strings
    '''   
    
    # Query for bounding box of boundary buffered 100 meters

    cmd = sql.SQL('''
    WITH buffer as
	    (
	    SELECT ST_BUFFER(ST_Transform(ST_SetSRID(geometry, 4326),
								      26915),
					     100) geom -- buff the geometry by 100 meters
	    FROM "Minneapolis Boundary"
	    ), bbox as
	    (
	    SELECT ST_EXTENT(ST_Transform(geom, 4326)) b
	    FROM buffer
	    )
    SELECT b::text
    FROM bbox;
    ''')

    response = psql.get_response(cmd, pg_connection_dict)
    
    # Gives a string
    # Unpack the response

    num_string = response[0][0][4:-1]
    
    # That's in xmin ymin, xmax ymax
    xmin = num_string.split(' ')[0]
    ymin = num_string.split(' ')[1].split(',')[0]
    xmax = num_string.split(' ')[1].split(',')[1]
    ymax = num_string.split(' ')[2]
    
    # Convert into PurpleAir API notation
    nwlng, selat, selng, nwlat = xmin, ymin, xmax, ymax
    
    return nwlng, selat, selng, nwlat
    

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 

def Get_newest_user(pg_connection_dict):
    '''
    This function gets the newest user's record_id
    
    returns an integer
    '''

    cmd = sql.SQL('''SELECT MAX(record_id)
    FROM "Sign Up Information";
    ''')

    response = psql.get_response(cmd, pg_connection_dict)
    
    if response[0][0] == None:
        max_record_id = 0
    else:
        max_record_id = response[0][0]
    
    return max_record_id

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
    FROM "Active Alerts Acute PurpleAir";
    ''')
    
    response = psql.get_response(cmd, pg_connection_dict)   
    # Convert response into dataframe
    
    cols_for_active_alerts = ['sensor_indices']
    active_alerts_df = pd.DataFrame(response, columns = cols_for_active_alerts)

    
    return active_alerts_df

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
    
### ~~~~~~~~~~~~~~~~~

##  New_Alerts

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

# Ended Alerts

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


