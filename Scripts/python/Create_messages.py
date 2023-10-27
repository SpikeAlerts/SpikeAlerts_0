### Import Packages

# File manipulation

import os # For working with Operating System
import requests # Accessing the Web
import datetime as dt # Working with dates/times

# Database 

import psycopg2
from psycopg2 import sql

# Analysis

import numpy as np
import geopandas as gpd
import pandas as pd
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def new_alert_message(sensor_index):
    '''
    Get a message for a new alert at a sensor_index
    # Composes and returns a single message
    '''
    
        
    # Short version (1 segment)
    
    message = f'''SPIKE ALERT!
Air quality is unhealthy in your area
https://map.purpleair.com/?select={sensor_index}/44.9723/-93.2447

Please reply STOP to end these alerts'''
        
    return message


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def end_alert_messages(durations, max_readings, report_ids, report_url):
    '''
    Get a list of messages to send when an alert is over
    This function returns a list of messages.
    All inputs must be iterables except report_url which links directly to REDCap comment survey
    '''
    
    
    messages = []
    
    for i in len(durations):
    
        message = f'''Alert Over
Duration: {durations[i]} minutes 
Max value: {max_readings[i]} ug/m3

To report, use Report Id {report_ids[i]} here:
{report_url}'''

        messages += [message]
        
    return messages
