# This is the main script for the AQ_Spike Alerts Project
# To run:
# 1) Please change directories to ./Scripts/python
# 2) Make sure you have the python environment activated (see Conda_Environment.yml for dependencies)
# 3) Call this script as follows: python MAIN.py spike_theshold_in_ugs days_to_run timestep_in_minutes
    # Example python MAIN.py 35 7 10
    # Will run the program for 7 days, update every 10 minutes, and alert people of sensors reading over 35 micrograms per meter cubed

### Import Libraries

# File/OS/Internet Interface

import os # For working with Operating System
import sys # System arguments
import requests # Accessing the Web
from dotenv import load_dotenv # Loading .env info
import time # For Sleeping

# Database 

import psycopg2
from psycopg2 import sql

# Analysis

import datetime as dt # Working with dates/times
import pytz # Timezones
import numpy as np
import pandas as pd
import geopandas as gpd

### Prep

## Load Functions
# Please see Scripts/python/*

exec(open('Get_spikes_df.py').read())
exec(open('Create_messages.py').read())
exec(open('twilio_functions.py').read())
exec(open('Update_Alerts.py').read())

## Definitions

load_dotenv() # Load .env file

# API Keys

purpleAir_api = os.getenv('PURPLEAIR_API_TOKEN') # PurpleAir API Read Key

redCap_token = os.getenv('REDCAP_TOKEN') # Survey Token

TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID') # Twilio Information
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')

# Database credentials

creds = [os.getenv('DB_NAME'),
         os.getenv('DB_USER'),
         os.getenv('DB_PASS'),
         os.getenv('DB_PORT'),
         os.getenv('DB_HOST')
        ]

pg_connection_dict = dict(zip(['dbname', 'user', 'password', 'port', 'host'], creds))  

## Other Constants from System Arguments

spike_threshold = int(sys.argv[1]) # Value which defines an AQ_Spike (Micgrograms per meter cubed)

# When to stop the program?
days_to_run = int(sys.argv[2]) # How many days will we run this?
timestep = int(sys.argv[3]) # Sleep time in between updates (in Minutes)
stoptime = dt.datetime.now() + dt.timedelta(days=days_to_run) # When to stop the program (datetime)


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`


### The Script

print(f'''Beginning program {sys.argv[0]}

Running for {days_to_run} days
Updating every {timestep} minutes
Spike Threshold = {spike_threshold}

''')

while True:

    now = dt.datetime.now() # The current time

    print(now)

    if stoptime < now: # Check if we've hit stoptime
        print("Terminating Program")
        break

    # Get the sensor_ids from sensors in our database

    sensor_ids = get_sensor_ids(pg_connection_dict) # In Get_Spikes_df.py

    # Check our active alerts

    active_alerts_df = get_active_alerts(pg_connection_dict) # In Update_Alerts.py

    # Query PurpleAir for Spikes

    spikes_df, runtime = Get_spikes_df(purpleAir_api, sensor_ids, spike_threshold) # In Get_Spikes_df.py

    # Sort the spiked sensors into new, ongoing, ended spiked sensors, and not spiked sensors

    new_spike_sensors, ongoing_spike_sensors, ended_spike_sensors, not_spiked_sensors = sort_sensors_for_updates(spikes_df, sensor_ids, pg_connection_dict) # In Update_Alerts.py

    # NEW Spikes
    
    if len(new_spike_sensors) > 0:

        new_spikes_df = spikes_df[spikes_df.sensor_index.isin(new_spike_sensors)]
    
        for index, row in new_spikes_df.iterrows():
    
            # 1) Add to active alerts
        
            add_to_active_alerts(row, pg_connection_dict,
                                 runtime.strftime('%Y-%m-%d %H:%M:%S') # When we ran the PurpleAir Query
                                )

            # 2) Text users about this - NOT DONE
            
        #print(
    
            # 3) Add to User's Active Alerts - NOT DONE

    # ONGOING spikes

    if len(ongoing_spike_sensors) > 0:

        ongoing_spikes_df = spikes_df[spikes_df.sensor_index.isin(ongoing_spike_sensors)]

        for _, spike in ongoing_spikes_df.iterrows():

            # 1) Update the maximum reading
    
            update_max_reading(spike, pg_connection_dict)
            
            # 2) Merge/Cluster alerts? - TO DO in the future

    # ENDED spikes

    if len(ended_spike_sensors) > 0:

        # 1) Add alert to archive
    
        add_to_archived_alerts(not_spiked_sensors, pg_connection_dict)

        # 2) Remove from Active Alerts
        
        ended_alert_indices = remove_active_alerts(not_spiked_sensors, pg_connection_dict)

        # 3) Text people it's over - NOT DONE

    # SLEEP

    time.sleep(timestep*60 - 3) # Sleep between updates - it takes about 3 seconds to run through everything

