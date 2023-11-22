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
from io import StringIO # String input/output
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

## Load our Functions

import Daily_Updates
import GetSort_Spikes
import New_Alerts
import Ongoing_Alerts
import Ended_Alerts
import Send_Alerts
import Twilio_Functions as our_twilio

## Global Variables

load_dotenv() # Load .env file

# API Keys

purpleAir_api = os.getenv('PURPLEAIR_API_TOKEN') # PurpleAir API Read Key

redCap_token_signUp = os.getenv('REDCAP_TOKEN_SIGNUP') # Survey Token
redCap_token_report = os.getenv('REDCAP_TOKEN_REPORT') # Report Token

TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID') # Twilio Information
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_NUMBER = os.getenv('TWILIO_NUMBER')

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
starttime = dt.datetime.now(pytz.timezone('America/Chicago')) 
stoptime = starttime + dt.timedelta(days=days_to_run)

# Waking hours
too_late_hr = 25 # 9pm
too_early_hr = -1 # 8am

# Report URL

base_report_url = 'https://redcap.ahc.umn.edu/surveys/?s=LN3HHDCJXYCKFCLE'

# Is Twilio number verified (can it send URLs)?

verified_number = True


### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`


### The Loop

print(f'''Beginning program {sys.argv[0]}

Running for {days_to_run} days
Updating every {timestep} minutes
Spike Threshold = {spike_threshold}

''')

# Initialize next update time (8am today) & storage for daily metrics

next_update_time = starttime.replace(hour=8, minute = 0, second = 0)
reports_for_day = 0
messages_sent_today = 0

while True:
#    try:
    now = dt.datetime.now(pytz.timezone('America/Chicago')) # The current time

    print(now)

    if stoptime < now: # Check if we've hit stoptime
        break
        
    # Is is within waking hours? Can we text people?
    if (now.hour < too_late_hr) & (now.hour > too_early_hr):
        can_text = True
    else:
        can_text = False

   # ~~~~~~~~~~~~~~~~~~~~~
   
   # Daily Updates
   
    if now > next_update_time:
    
        next_update_time, reports_for_day, messages_sent_today = Daily_Updates.workflow(next_update_time,
                                                                                        reports_for_day,
                                                                                  messages_sent_today,
                                                                                   purpleAir_api,
                                                                                    redCap_token_signUp,
                                                                                    pg_connection_dict)
   
   # ~~~~~~~~~~~~~~~~~~~~~

    # Query PurpleAir for Spikes and sort out if we have new, ongoing, ended, flagged, not spiked sensors

    spikes_df, purpleAir_runtime, sensors_dict = GetSort_Spikes.workflow(purpleAir_api, pg_connection_dict, spike_threshold)


    # Initialize message/record_id storage
    
    record_ids_to_text = []
    messages = []
    
    # ~~~~~~~~~~~~~~~~~~~~~
    
    # NEW Spikes
    
    if len(sensors_dict['new']) > 0:

        new_spikes_df = spikes_df[spikes_df.sensor_index.isin(sensors_dict['new'])] 
    
        messages, record_ids_to_text = New_Alerts.workflow(new_spikes_df, purpleAir_runtime, messages, record_ids_to_text, can_text, pg_connection_dict)
                 
    # ~~~~~~~~~~~~~~~~~~~~~

    # ONGOING spikes

    if len(sensors_dict['ongoing']) > 0:

        ongoing_spikes_df = spikes_df[spikes_df.sensor_index.isin(sensors_dict['ongoing'])]
        
        Ongoing_Alerts.workflow(ongoing_spikes_df, pg_connection_dict)
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~
    
    # ENDED spikes

    messages, record_ids_to_text, reports_for_day = Ended_Alerts.workflow(sensors_dict,
                                                                         purpleAir_runtime,
                                                                          messages,
                                                                           record_ids_to_text,
                                                                            reports_for_day,
                                                                            base_report_url,
                                                                            can_text,
                                                                             pg_connection_dict)
                
    # ~~~~~~~~~~~~~~~~~~~~~           
    
    # Send all messages
    
    if len(record_ids_to_text) > 0:
    
        Send_Alerts.send_all_messages(record_ids_to_text, messages,
                          redCap_token_signUp,
                          TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_NUMBER,
                          pg_connection_dict) # in Send_Alerts.py & .ipynb
        
        # Save them locally - for developers
        
        f = open("test.txt", "a")
        for i in range(len(record_ids_to_text)):
            line = f'\n\n{str(record_ids_to_text[i])} - {purpleAir_runtime}\n\n' + messages[i]
            f.write(line)
        f.close()
        
        messages_sent_today += len(record_ids_to_text) # Not quite right. Overcounts for unsubscribed numbers
    
    # ~~~~~~~~~~~~~~~~~~~~~

    # SLEEP between updates

    when_to_awake = now + dt.timedelta(minutes=timestep) 

    sleep_seconds = (when_to_awake - dt.datetime.now(pytz.timezone('America/Chicago'))).seconds # - it takes about 3 seconds to run through everything without texting... I think?

    time.sleep(sleep_seconds) # Sleep
        
#    except Exception as e:
#        our_twilio.send_texts([os.environ['LOCAL_PHONE']], ['SpikeAlerts Down'], TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_NUMBER)
#        print(e)
#        answer = input('\n\nWhat would you like to do? (type continue to resume)')
#        if answer == 'continue':
#            pass
#        else:
#            print(reports_for_day, 'reports so far today')
#            print(messages_sent_today, 'messages so far today')
#            break
       
# ~~~~~~~~~~~~~~~~~~~~~

# Terminate Program

#our_twilio.send_texts([os.environ['LOCAL_PHONE']], ['Terminating Program'], TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_NUMBER)

print("Terminating Program")
