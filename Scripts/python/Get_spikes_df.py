### Import Packages

# File manipulation

import os # For working with Operating System
import requests # Accessing the Web
import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 

import psycopg2
from psycopg2 import sql

# Analysis

import numpy as np
import geopandas as gpd
import pandas as pd

### Function to get the sensor_ids from our database

def get_sensor_ids(pg_connection_dict):
    '''
    This function gets the sensor_ids of all sensors in our database that are not flagged from previous days.
    Returns a pandas Series
    '''

    # Connect
    conn = psycopg2.connect(**pg_connection_dict) 
    # Create cursor
    cur = conn.cursor()

    cmd = sql.SQL('''SELECT sensor_index 
    FROM "PurpleAir Stations"
    WHERE channel_flags = ANY (ARRAY[0,4]) AND channel_state = 3; -- channel_flags are updated regularly, channel_state <- managed by someone - 3 = on, 0 = off
    ''')

    cur.execute(cmd) # Execute
    conn.commit() # Committ command

    # Unpack response into pandas series

    sensor_ids = pd.DataFrame(cur.fetchall(), columns = ['sensor_index']).sensor_index

    # Close cursor
    cur.close()
    # Close connection
    conn.close()

    return sensor_ids
    
### Function to flag sensors in our database

def flag_sensors(sensor_indices, pg_connection_dict):
    '''
    This function sets the channel_flags = 4 in our database on the given sensor_indices (list)
    '''

    # Connect
    conn = psycopg2.connect(**pg_connection_dict) 
    # Create cursor
    cur = conn.cursor()

    cmd = sql.SQL('''UPDATE "PurpleAir Stations"
    SET channel_flags = 4, last_seen = CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago'
    WHERE sensor_index = ANY ( {} );
    ''').format(sql.Literal(sensor_indices))

    cur.execute(cmd) # Execute
    conn.commit() # Committ command

    # Close cursor
    cur.close()
    # Close connection
    conn.close()

    return

# Function to get Sensors Data from PurpleAir

def getSensorsData(query='', api_read_key=''):

    # my_url is assigned the URL we are going to send our request to.
    url = 'https://api.purpleair.com/v1/sensors?' + query

    # my_headers is assigned the context of our request we want to make. In this case
    # we will pass through our API read key using the variable created above.
    my_headers = {'X-API-Key':api_read_key}

    # This line creates and sends the request and then assigns its response to the
    # variable, r.
    response = requests.get(url, headers=my_headers)

    # We then return the response we received.
    return response

### The Function to get spikes dataframe

def Get_spikes_df(purpleAir_api, sensor_ids, spike_threshold):
    
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

    fields_string = 'fields=' + '%2C'.join(fields)
       
    sensor_string = 'show_only=' + '%2C'.join(sensor_ids.astype(str))

    query_string = '&'.join([fields_string, sensor_string])
    
    ### Call the api
    
    response = getSensorsData(query_string, purpleAir_api) # The response is a requests.response object
    runtime = dt.datetime.now(pytz.timezone('America/Chicago')) # When we call - datetime in our timezone
    
    if response.status_code != 200:
        print('ERROR in PurpleAir API Call')
        print('HTTP Status: ' + str(response.status_code))
        print(response.text)
        
    else:
        response_dict = response.json() # Read response as a json (dictionary)

        col_names = response_dict['fields']
        data = np.array(response_dict['data'])

        sensors_df = pd.DataFrame(data, columns = col_names) # Format as Pandas dataframe
        
        # Correct last_seen

        sensors_df['last_seen'] = pd.to_datetime(sensors_df['last_seen'],
                                                 utc = True,
                                                 unit='s').dt.tz_convert('America/Chicago')
                                                 
        # Correct sensor_index/channel_flags

        sensors_df['sensor_index'] = sensors_df['sensor_index'].astype(int)
        sensors_df['channel_flags'] = sensors_df['channel_flags'].astype(int)
    
        ### Clean the data
        
        # Key
        # Channel Flags - 0 = Normal, 1 = A Downgraded, 2 - B Downgraded, 3 - Both Downgraded
        # last seen in the last hour is also a flag
        
        flags = (sensors_df.channel_flags != 0 
              ) |(sensors_df.last_seen < dt.datetime.now(pytz.timezone('America/Chicago')) - dt.timedelta(minutes=60)
                     )

        
        clean_df = sensors_df[~flags].copy()

        # Rename column for ease of use

        clean_df = clean_df.rename(columns = {'pm2.5_10minute':'pm25'})

        # Remove obvious error values

        clean_df = clean_df[clean_df.pm25 < 1000] 

        # Remove NaNs

        clean_df = clean_df.dropna()
        
        ### Get spikes_df
        
        spikes_df = clean_df[clean_df.pm25 >= spike_threshold][['sensor_index', 'pm25']].reset_index(drop=True) 
        
        ### Get Flagged Sensors
            
        flagged_df = sensors_df[flags].copy()

        flagged_sensor_ids = flagged_df.reset_index(drop=True).sensor_index

        # Flag them in our database

        flag_sensors(flagged_sensor_ids.to_list(), pg_connection_dict)
    
    return spikes_df, runtime, flagged_sensor_ids
