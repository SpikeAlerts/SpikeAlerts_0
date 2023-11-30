### Import Packages

# File manipulation

# Purple Air

from App.modules import PurpleAir_Functions as purp

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 

from App.modules import Our_Queries as query
from App.modules import Basic_PSQL as psql
from psycopg2 import sql

# Data Manipulation

import pandas as pd

## Workflow

def workflow(purpleAir_api, pg_connection_dict, spike_threshold):
    '''
    Runs the full workflow to get and sort the current spikes
    
    returns spikes_df (pd.DataFrame with columns 'sensor_index' int  and 'pm25' float, 
            purpleAir_runtime (datetime timestamp),
            sensors_dict (dictionary with keys new/ongoing/ended/not/flagged
                                         values are sets of sensor_indices integers)
    '''
    
    #  Get the sensor_ids from sensors in our database that are not flagged

    sensor_ids = query.Get_sensor_ids(pg_connection_dict) # In Get_Spikes_df.py

    # Query PurpleAir for Spikes

    spikes_df, purpleAir_runtime, flagged_sensor_ids = Get_spikes_df(purpleAir_api, sensor_ids, spike_threshold) # In Get_Spikes_df.py
    
    # Update last_elevated
    
    if len(spikes_df) > 0:
        
        Update_last_elevated(spikes_df.sensor_index.to_list(), purpleAir_runtime, pg_connection_dict)
    
    if len(flagged_sensor_ids) > 0:
        # Flag sensors in our database (set channel_flags = 4 for the list of sensor_index)

        flag_sensors(flagged_sensor_ids.to_list(), pg_connection_dict)    
    
    sensors_dict = Sort_sensor_indices(spikes_df, flagged_sensor_ids, pg_connection_dict)
    
    return spikes_df, purpleAir_runtime, sensors_dict

### The Function to get spikes dataframe

def Get_spikes_df(purpleAir_api, sensor_ids, spike_threshold, timezone = 'America/Chicago'):
    
    ''' This function queries the PurpleAir API for sensors in the list of sensor_ids for readings over a spike threshold. 
    It will return a pandas dataframe with columns sensor_index (integer) and pm25 (float) as well as a runtime (datetime)
    
    Inputs:
    
    api = string of PurpleAir API api_read_key
    sensor_ids = list of integers of purpleair sensor ids to query
    spike_threshold = float of spike value threshold (keep values >=)
    
    Outputs:
    
    spikes_df = Pandas DataFrame with fields sensor_index (integer) and pm25 (float)
    runtime = datetime object when query was run
    flagged_sensors = Pandas Series of sensor_indices that came up flagged
    '''
    
    ### Setting parameters for API
    
    fields = ['pm2.5_10minute', 'channel_flags', 'last_seen']

    sensors_df, runtime = purp.Get_PurpleAir_df_sensors(purpleAir_api, sensor_ids, fields)
        
    if len(sensors_df) > 0:
        # Correct last_seen
        sensors_df['last_seen'] = pd.to_datetime(sensors_df['last_seen'],
                                                 utc = True,
                                                 unit='s').dt.tz_convert(timezone)                                
        # Correct sensor_index/channel_flags
        sensors_df['sensor_index'] = sensors_df['sensor_index'].astype(int)
        sensors_df['channel_flags'] = sensors_df['channel_flags'].astype(int)
        ### Clean the data
        # Key
        # Channel Flags - 0 = Normal, 1 = A Downgraded, 2 - B Downgraded, 3 - Both Downgraded
        # last seen not in the last hour is also a flag
        flags = (sensors_df.channel_flags != 0 
                ) |(sensors_df.last_seen < dt.datetime.now(pytz.timezone(timezone)) - dt.timedelta(minutes=60)
                     )
        clean_df = sensors_df[~flags].copy()

        # Rename columns for ease of use
        clean_df = clean_df.rename(columns = {'pm2.5_10minute':'pm25'})
        # Remove obvious error values
        clean_df = clean_df[clean_df.pm25 < 1000] 
        # Remove NaNs
        clean_df = clean_df.dropna()
        
        ### Get spikes_df
        spikes_df = clean_df[clean_df.pm25 >= spike_threshold][['sensor_index', 'pm25']].reset_index(drop=True) 
        
        ### Get Flagged Sensor_ids
        flagged_df = sensors_df[flags].copy()
        flagged_sensor_ids = flagged_df.reset_index(drop=True).sensor_index
        
    else: # Returns empty if errored
        spikes_df = sensors_df
        flagged_sensor_ids = pd.Series()
    
    return spikes_df, runtime, flagged_sensor_ids
    
### Function to update all last_elevateds

def Update_last_elevated(sensor_indices, purpleAir_runtime, pg_connection_dict):
    '''
    This function updates all the sensors' last_elevated that are currently spikes
    '''
    update_time = purpleAir_runtime.strftime('%Y-%m-%d %H:%M:%S')
    
    cmd = sql.SQL('''UPDATE "PurpleAir Stations"
    SET last_elevated = {}
    WHERE sensor_index = ANY ( {} );
    '''
    ).format(sql.Literal(update_time),
            sql.Literal(sensor_indices))
            
    psql.send_update(cmd, pg_connection_dict)
    
### Function to flag sensors in our database

def flag_sensors(sensor_indices, pg_connection_dict):
    '''
    This function sets the channel_flags = 4 in our database on the given sensor_indices (list)
    '''

    cmd = sql.SQL('''UPDATE "PurpleAir Stations"
    SET channel_flags = 4, last_seen = CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago'
    WHERE sensor_index = ANY ( {} );
    ''').format(sql.Literal(sensor_indices))

    psql.send_update(cmd, pg_connection_dict)
    
### Function to sort the sensor indices
    
def Sort_sensor_indices(spikes_df, flagged_sensor_ids, pg_connection_dict):
    '''
    This sorts the sensor indices into sets based on if they are new, ongoing, ended, flagged, or not spiked
    
    Inputs: spikes_df - pd.DataFrame - from Get_spikes_df()
            sensor_ids - list of integers - from get_sensor_ids()
            active_alerts_df - pd.DataFrame - from get_active_alerts()
            
    returns a dictionary with keys 'new', 'ongoing', 'ended', 'flagged', or 'not'
                values are sets of integers (sensor_index)
    '''
    
    # Initialize storage
    
    sensor_dict = {'new': set(),
                   'ongoing': set(),
                   'ended': set(),
                   'flagged': set(flagged_sensor_ids),
                   'not': set()
                    }
    
    # Check for 4 types of Sensor ID
    # Using set operations between
    
    # Currently active
    current_active_spike_sensors = set(spikes_df.sensor_index) # From most recent api call

    # Previously active
    active_alerts_df = query.Get_previous_active_sensors(pg_connection_dict)
    
    if len(active_alerts_df) > 0:
        previous_active_spike_sensors = set(active_alerts_df.sensor_indices.sum()) # From our database - sensor_indices are currently composed of arrays
        # The sensor_indices are given as lists of indices because we may cluster alerts eventually
    else:
        previous_active_spike_sensors = set()
        
    # Not active  = all sensors not elevated in past 30 minutes
    
    not_elevated_sensor_indices = set(query.Get_not_elevated_sensors(pg_connection_dict))

    # The sets:

    # 1) new
    sensor_dict['new'] = current_active_spike_sensors - previous_active_spike_sensors

    # 2) ongoing, 
    sensor_dict['ongoing'] = current_active_spike_sensors.intersection(previous_active_spike_sensors)

    # 3) Ended alerted sensors
    sensor_dict['ended'] = not_elevated_sensor_indices.intersection(previous_active_spike_sensors)

    # 4) Not Spiked = all sensors not elevated in past 30 minutes
    sensor_dict['not'] = not_elevated_sensor_indices
    
    return sensor_dict
    
  
