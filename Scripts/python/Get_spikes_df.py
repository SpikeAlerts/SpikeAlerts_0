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
import pandas as pd

### Function to get the sensor_ids from our database

def get_sensor_ids(pg_connection_dict):
    '''
    This function gets the sensor_ids of all sensors in our database
    '''

    # Connect
    conn = psycopg2.connect(**pg_connection_dict) 
    # Create cursor
    cur = conn.cursor()

    cmd = sql.SQL('''SELECT sensor_index 
    FROM "PurpleAir Stations"
    ''')

    cur.execute(cmd) # Execute
    conn.commit() # Committ command

    # Unpack response into numpy array

    sensor_ids = pd.DataFrame(cur.fetchall(), columns = ['sensor_index']).sensor_index

    # Close cursor
    cur.close()
    # Close connection
    conn.close()

    return sensor_ids

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

def Get_spikes_df(api, sensor_ids, spike_threshold):
    
    ''' This function queries the PurpleAir API for sensors in the list of sensor_ids for readings over a spike threshold. 
    It will return a pandas dataframe with columns sensor_index (integer) and pm25 (float) as well as a runtime (datetime)
    
    Inputs:
    
    api = string of PurpleAir API api_read_key
    sensor_ids = list of integers of purpleair sensor ids to query
    spike_threshold = float of spike value threshold (keep values >=)
    
    Outputs:
    
    spikes_df = Pandas DataFrame with fields sensor_index (integer) and pm25 (float)
    runtime = datetime object when query was run
    '''
    
    ### Setting parameters for API
    
    fields = ['pm2.5_10minute']

    fields_string = 'fields=' + '%2C'.join(fields)
       
    sensor_string = 'show_only=' + '%2C'.join(sensor_ids.astype(str))

    query_string = '&'.join([fields_string, sensor_string])
    
    ### Call the api
    
    runtime = dt.datetime.now(pytz.timezone('America/Chicago')) # The time the query is sent
    response = getSensorsData(query_string, api)
    
    response_dict = response.json() # Read response as a json (dictionary)

    col_names = response_dict['fields']
    data = np.array(response_dict['data'])

    sensors_df = pd.DataFrame(data, columns = col_names) # Format as Pandas dataframe
    
    ### Clean the data
    
    clean_df = sensors_df.copy()

    # Rename column for ease of use

    clean_df = clean_df.rename(columns = {'pm2.5_10minute':'pm25'})

    # Remove obvious error values

    clean_df = clean_df[clean_df.pm25 < 1000] 

    # Remove NaNs

    clean_df = clean_df.dropna()
    
    ### Get spikes_df
    
    spikes_df =  clean_df[clean_df.pm25 >= spike_threshold]
    
    return spikes_df, runtime
