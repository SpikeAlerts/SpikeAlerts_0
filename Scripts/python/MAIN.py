# This is the main script for the AQ_Spike Alerts Project
# To run:
# 1) Please change directories to ./Scripts/python
# 2) Make sure you have the python environment activated (see Conda_Environment.yml for dependencies)
# 3) Call this script as follows: python MAIN.py spike_theshold_in_ugs days_to_run timestep_in_minutes
    # Example python MAIN.py 35 7 10
    # Will run the program for 7 days, update every 10 minutes, and alert people of sensors reading over 35 micrograms per meter cubed

### Prep

## Import Libraries

# File Manipulation

import os # For working with Operating System
import sys # System arguments
from dotenv import load_dotenv # Loading .env info

# Web

import requests # Accessing the Web

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones
import time # For Sleeping

# Database 

import psycopg2
from psycopg2 import sql

# Data Manipulation

import numpy as np
import geopandas as gpd
import pandas as pd

## Load Functions

# Please see Scripts/python/*
exec(open('Get_spikes_df.py').read())
exec(open('Create_messages.py').read())
exec(open('twilio_functions.py').read())
exec(open('Update_Alerts.py').read())
exec(open('Send_Alerts.py').read())

## Global Variables

load_dotenv() # Load .env file

# API Keys

purpleAir_api = os.getenv('PURPLEAIR_API_TOKEN') # PurpleAir API Read Key

redCap_token_signUp = os.getenv('REDCAP_TOKEN_SIGNUP') # Survey Token
redCap_token_report = os.getenv('REDCAP_TOKEN_REPORT') # Report Token

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

timestep = int(sys.argv[3]) # Sleep time in between updates (in Minutes)

# When to stop the program? (datetime)
days_to_run = int(sys.argv[2]) # How many days will we run this?
stoptime = dt.datetime.now() + dt.timedelta(days=days_to_run)

# Waking hours
too_late_hr = 21 # 9pm
too_early_hr = 8 # 8am


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`


### The Loop

print(f'''Beginning program {sys.argv[0]}

Running for {days_to_run} days
Updating every {timestep} minutes
Spike Threshold = {spike_threshold}

''')

while True:

    now = dt.datetime.now() # The current time

    print(now)

    if stoptime < now: # Check if we've hit stoptime
        break
        
#    if new day:
#        NOT DONE

    #  Get the sensor_ids from sensors in our database

    sensor_ids = get_sensor_ids(pg_connection_dict) # In Get_Spikes_df.py

    # Query PurpleAir for Spikes

    spikes_df, runtime, flagged_sensor_ids = Get_spikes_df(purpleAir_api, sensor_ids, spike_threshold) # In Get_Spikes_df.py

    # Sort the spiked sensors into new, ongoing, ended spiked sensors, and not spiked sensors

    new_spike_sensors, ongoing_spike_sensors, ended_spike_sensors, not_spiked_sensors = sort_sensors_for_updates(spikes_df, sensor_ids, flagged_sensor_ids, pg_connection_dict) # In Update_Alerts.py

    # Initialize message/record_id storage
    
    record_ids_to_text = []
    messages = []
    
    # NEW Spikes
    
    if len(new_spike_sensors) > 0:

        new_spikes_df = spikes_df[spikes_df.sensor_index.isin(new_spike_sensors)] 
    
        for index, row in new_spikes_df.iterrows():
    
            # 1) Add to active alerts
        
            newest_alert_index = add_to_active_alerts(row, pg_connection_dict,
                                 runtime.strftime('%Y-%m-%d %H:%M:%S') # When we ran the PurpleAir Query
                                ) # In Update_Alerts.py
            
            # 2) Query users ST_Dwithin 1000 meters & subscribed = TRUE
            
            record_ids_nearby = Users_nearby_sensor(pg_connection_dict, row.sensor_index, 1000) # in Send_Alerts.py
            
            if len(record_ids_nearby) > 0:

                if (now.hour < too_late_hr) & (now.hour > too_early_hr):
            
                    # a) Query users from record_ids_nearby if both active_alerts and cached_alerts are empty
                    record_ids_new_alerts = Users_to_message_new_alert(pg_connection_dict, record_ids_nearby) # in Send_Alerts.py & .ipynb 
                    
                    # Compose Messages & concat to messages/record_id_to_text   
                    
                    # Add to message/record_id storage for future messaging
                    record_ids_to_text += record_ids_new_alerts
                    messages += [new_alert_message(sensor_id)]*len(record_ids_new_alerts) # in Compose_Messages.py


                # b) Add newest_alert_index to record_ids_nearby's Active Alerts
            # - NOT DONE - do in Update_Alerts.py & .ipynb

    # ONGOING spikes

    if len(ongoing_spike_sensors) > 0:

        ongoing_spikes_df = spikes_df[spikes_df.sensor_index.isin(ongoing_spike_sensors)]

        for _, spike in ongoing_spikes_df.iterrows():

            # 1) Update the maximum reading
    
            update_max_reading(spike, pg_connection_dict) # In Update_Alerts.py
            
            # 2) Merge/Cluster alerts? 
            # NOT DONE - FAR FUTURE TO DO

    # ENDED spikes

    if len(ended_spike_sensors) > 0:

        # 1) Add alert to archive
    
        add_to_archived_alerts(not_spiked_sensors, pg_connection_dict) # In Update_Alerts.py

        # 2) Remove from Active Alerts
        
        ended_alert_indices = remove_active_alerts(not_spiked_sensors, pg_connection_dict) # # A list from Update_Alerts.py

        # 3) Transfer these alerts from "Sign Up Information" active_alerts to "Sign Up Information" cached_alerts 
        # NOT DONE - do in Update_Alerts.py & .ipynb

        # 4) Query for people to text (subscribed = TRUE and active_alerts is empty and cached_alerts not empty and cached_alerts is > 10 minutes old - ie. ended_alert_indices intersect cached_alerts is empty) 
        # NOT DONE - do in Send_Alerts.py & .ipynb

        # 5) If #4 has elements: for each element (user) in #4
            
            # a) Initialize report - generate unique report_id, log cached_alerts and use to find start_time/max reading/duration/sensor_indices
            # - NOT DONE - do in Send_Alerts.py & .ipynb
    
            # b) Compose message telling user it's over w/ unique report option & concat to messages/record_id_to_text
            # - NOT DONE - do in Send_Alerts.py & .ipynb

            # c) Clear the user's cached_alerts 
            # - NOT DONE - do in Update_Alerts.py & .ipynb

    # SLEEP between updates

    when_to_awake = now + dt.timedelta(minutes=timestep) 

    sleep_seconds = (when_to_awake - dt.datetime.now()).seconds # - it takes about 3 seconds to run through everything

    time.sleep(sleep_seconds) # Sleep


print("Terminating Program")
