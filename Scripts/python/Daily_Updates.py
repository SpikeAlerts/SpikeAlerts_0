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
