# Functions to interface with PurpleAir

## Load modules

import requests

# Time

import datetime as dt
import pytz # Timezones

# Data Manipulation

import numpy as np
import pandas as pd


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
    
### The Function to get a dataframe from purpleair for select sensor_ids

def Get_PurpleAir_df(purpleAir_api, sensor_ids, fields, timezone = 'America/Chicago'):
    
    ''' This function queries the PurpleAir API for sensors in the list of sensor_ids for readings over a spike threshold. 
    It will return an unformatted pandas dataframe with the specified fields as well as a runtime (datetime)
    
    Inputs:
    
    api = string of PurpleAir API api_read_key
    sensor_ids = list of integers of purpleair sensor ids to query
    fields - list of strings that line up with PurpleAir api
    
    Outputs:
    
    df = Pandas DataFrame with fields (datatypes not formatted!)
    runtime = datetime object when query was run
    '''
    
    ### Setting parameters for API
    fields_string = 'fields=' + '%2C'.join(fields)
    sensor_string = 'show_only=' + '%2C'.join(pd.Series(sensor_ids).astype(str))

    query_string = '&'.join([fields_string, sensor_string])
    
    ### Call the api
    
    response = getSensorsData(query_string, purpleAir_api) # The response is a requests.response object
    runtime = dt.datetime.now(pytz.timezone(timezone)) # When we call - datetime in our timezone
    
    if response.status_code != 200:
        print('ERROR in PurpleAir API Call')
        print('HTTP Status: ' + str(response.status_code))
        print(response.text)
        
        df = pd.DataFrame()
        
    else:
        response_dict = response.json() # Read response as a json (dictionary)
        col_names = response_dict['fields']
        data = np.array(response_dict['data'])

        df = pd.DataFrame(data, columns = col_names) # Format as Pandas dataframe
            
    return df, runtime
