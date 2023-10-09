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

def sort_sensors_for_updates(spikes_df, pg_connection_dict):
    '''
    This sorts the sensor indices into sets based on if they are new, ongoing, or ended
    spikes_df comes from Get_spikes_df() in Get_Spikes_df.py
    
    '''
    
    # Get active alerts from database

    conn = psycopg2.connect(**pg_connection_dict)

    # Create json cursor
    cur = conn.cursor()

    cmd = sql.SQL('''SELECT * 
    FROM "Active Alerts Acute PurpleAir"
    ''')

    cur.execute(cmd) # Execute

    conn.commit() # Committ command

    # Convert response into dataframe

    cols_for_active_alerts = ['alert_index', 'sensor_index', 'start_time', 'max_reading']
    active_alerts = pd.DataFrame(cur.fetchall(), columns = cols_for_active_alerts)
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
    # Check for:

    # 1) new,
    # 2) ongoing, 
    # 3) and ended alerts

    # (set differences or intersection between sensor indices)

    current_active_spike_sensors = set(spikes_df.sensor_index) # From most recent api call
    previous_active_spike_sensors = set(active_alerts.sensor_index.astype(int)) # From our database

    # The sets of sensor indices (new, ended, ongoing)
    new_spike_sensors = current_active_spike_sensors - previous_active_spike_sensors
    ongoing_spike_sensors = current_active_spike_sensors.intersection(previous_active_spike_sensors)
    ended_spike_sensors = previous_active_spike_sensors - current_active_spike_sensors
    
    return new_spike_sensors, ongoing_spike_sensors, ended_spike_sensors
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~    
    
# For each NEW alert, we should:

# 1) Add to active alerts

def add_to_active_alerts(row, pg_connection_dict, cols_for_db, runtime_for_db):
    '''
    This takes a row from spikes_df[spikes_df.sensor_index.isin(new_spike_sensors)],
    the connection dictionary,
    cols_for_db = column names of database as a list (['sensor_index', 'start_time', 'max_reading'])
    runtime_for_db = when purpleair was queried as a string (runtime.strftime('%Y-%m-%d %H:%M:%S'))
    
    '''

    sensor_index = row.sensor_index
    reading = row.pm25

    # 1) Add to active alerts

    # Create Cursor for commands
    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()
    
    # This is really a great way to insert a lot of data

    vals = [sensor_index, runtime_for_db, reading]
    
    q1 = sql.SQL('INSERT INTO "Active Alerts Acute PurpleAir" ({}) VALUES ({});').format(
     sql.SQL(', ').join(map(sql.Identifier, cols_for_db)),
     sql.SQL(', ').join(sql.Placeholder() * (len(cols_for_db))))

    cur.execute(q1.as_string(conn),
        (vals)
        )
    # Commit command
    conn.commit()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  

# For each ONGOING alert, we should

# 1) Update max_reading if it's higher

def update_max_reading(row, pg_connection_dict):
    '''
    Row should be a row from the ongoing_spikes dataFrame
    eg. spikes_df[spikes_df.sensor_index.isin(ongoing_spike_sensors)]
    '''

    sensor_index = row.sensor_index
    reading = row.pm25

    # 1) Add to active alerts

    # Create Cursor for commands
    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()
    
    cmd = sql.SQL('''
UPDATE "Active Alerts Acute PurpleAir"
SET max_reading = GREATEST({}, max_reading)
WHERE sensor_index = {};

''').format(sql.Literal(reading), sql.Literal(sensor_index))

    cur.execute(cmd)
    # Commit command
    conn.commit()

    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
  
 # For each ENDED alert, we should

# 1) Add to archived alerts

def add_to_archived_alerts(ended_spike_sensors, pg_connection_dict):
    '''
    ended_spike_sensors is a set of sensor indices that have ended spikes Alerts
    '''

    # Get relevant sensor indices as list
    sensor_indices = list(ended_spike_sensors)
                           
    # Create Cursor for commands
    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()

    # This command selects the ended alerts from active alerts
    # Then it gets the difference from the current time and when it started
    # Lastly, it inserts this selection while converting that time difference into minutes for duration_minutes column
    cmd = sql.SQL('''
    WITH ended_alerts as
    (
SELECT alert_index, sensor_index, start_time, CURRENT_TIMESTAMP - start_time as time_diff, max_reading 
FROM "Active Alerts Acute PurpleAir"
WHERE sensor_index IN (SELECT UNNEST({}))
    )
    INSERT INTO "Archived Alerts Acute PurpleAir" 
    SELECT alert_index, sensor_index, start_time, (((DATE_PART('day', time_diff) * 24) + 
    DATE_PART('hour', time_diff)) * 60 + DATE_PART('minute', time_diff)) as duration_minutes, max_reading
    FROM ended_alerts;
    ''').format(sql.Literal(sensor_indices))
    
    cur.execute(cmd)
    # Commit command
    conn.commit()
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    

#~~~~~~~~~~~~~~~~

# 2) Remove from active alerts

def remove_active_alerts(ended_spike_sensors, pg_connection_dict):
    '''
    This function removes the ended_spikes from the Active Alerts Table
    It also retrieves their alert_index
    
    ended_spike_sensors is a set of sensor indices that have ended spikes Alerts
    
    ended_alert_indices is returned alert_indices (as a list) of the removed alerts for accessing Archive for end message 
    
    '''

    # Get relevant sensor indices as list
    sensor_indices = list(ended_spike_sensors)
                           
    # Create Cursor for commands
    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()
    
    cmd = sql.SQL('''
    SELECT alert_index
    FROM "Active Alerts Acute PurpleAir"
    WHERE sensor_index IN (SELECT UNNEST({}));
    ''').format(sql.Literal(sensor_indices))
    
    cur.execute(cmd)
    # Commit command
    conn.commit()
    
    ended_alert_indices = list(cur.fetchall()[0])
    
    cmd = sql.SQL('''
    DELETE FROM "Active Alerts Acute PurpleAir"
    WHERE sensor_index IN (SELECT UNNEST({}));
    ''').format(sql.Literal(sensor_indices))
    
    cur.execute(cmd)
    # Commit command
    conn.commit()
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()   
    
    return ended_alert_indices
    
    
    
    
