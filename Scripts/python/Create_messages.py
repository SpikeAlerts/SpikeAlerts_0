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

Text STOP to unsubscribe'''
        
    return message


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def end_alert_message(duration, max_reading, report_id, base_report_url):
    '''
    Get a list of messages to send when an alert is over

    inputs:
    duration = integer (number of minutes)
    max_reading = float
    report_id = string
    base_report_url is a string links directly to REDCap comment survey
    
    Returns a message (string)
    '''
        
    message = f'''Alert Over
Duration: {duration} minutes 
Max value: {max_reading} ug/m3

Report here - {base_report_url+ '&report_id=' + report_id}'''
    # See https://help.redcap.ualberta.ca/help-and-faq/survey-parameters for filling in variable in url
        
    return message
