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
import pandas as pd


def Refresh_SensorFlags(pg_connection_dict):
    '''
    Sets all channel_flags to zero in "PurpleAir Stations"
    '''
    
    conn = psycopg2.connect(**pg_connection_dict)
    
    # Create json cursor
    cur = conn.cursor()
    
    cmd = sql.SQL('''UPDATE "PurpleAir Stations"
SET channel_flags = 0;
    ''')
    
    cur.execute(cmd) # Execute
    
    conn.commit() # Committ command
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
def Unsubscribe_users(record_ids, pg_connection_dict):
    '''
    Change record_ids to subscribed = FALSE in our database <- checked before every message, not daily... 
    '''
    
    # Our Database
    
    conn = psycopg2.connect(**pg_connection_dict)
    
    # Create json cursor
    cur = conn.cursor()
    
    cmd = sql.SQL('''UPDATE "Sign Up Information"
    SET subscribed = FALSE
WHERE record_id = ANY ( {} );
    ''').format(sql.Literal(record_ids))
    
    cur.execute(cmd) # Execute
    
    conn.commit() # Committ command
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
#    # REDCap <- need permissions to delete records
#    
#    # Select by record_id <- probably not the best way, but I couldn't get 'record' to work properly in data
#    
#    record_id_strs = [str(record_id) for record_id in record_ids]
#    filterLogic_str = '[record_id]=' + ' OR [record_id]='.join(record_id_strs)
#    
#    data = {
#    'token': redCap_token_signUp,
#    'content': 'record',
#    'fields' : field_names,
#    'action': 'delete',
#    'filterLogic': filterLogic_str  
#    }
#    r = requests.post('https://redcap.ahc.umn.edu/api/',data=data)    
