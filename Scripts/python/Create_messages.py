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

def degrees_to_cardinal(degree):
    '''
    A Function thtat gets the cardinal direction from a degree (azimuth)
    note: this is highly approximate...
    from https://gist.github.com/RobertSudwarts/acf8df23a16afdb5837f
    '''
    dirs = np.array(["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                     "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"], dtype='U')
    ix = int(np.round(degree / (360. / len(dirs)))) % 16
    return dirs[ix]
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def new_alert_messages(sensor_indices, readings, intersection_indices):
    '''
    Get a list of messages for new alerts
    # all inputs should be lists!
    # Composes the messages and return as a list of messages
    '''
    ## Database Credentials

    cred_pth = os.path.join(os.getcwd(), '..', '..', 'Scripts', 'database', 'db_credentials.txt')

    with open(cred_pth, 'r') as f:
        
        creds = f.readlines()[0].rstrip('\n').split(', ')

    pg_connection_dict = dict(zip(['dbname', 'user', 'password', 'port', 'host'], creds))
    
    ## Query for relevant information
    
    conn = psycopg2.connect(**pg_connection_dict)

    # Create json cursor
    cur = conn.cursor()

    # Get direction and distance from intersection to the sensors using POSTGIS
    cmd = sql.SQL('''
    WITH sensor_data as
    (
    SELECT * 
    FROM "PurpleAir Stations" p
    JOIN unnest({}::int[])
    WITH ORDINALITY t(sensor_index, ord)
    USING (sensor_index)
    ORDER BY t.ord
    ),
    intersection_data as
    (
    SELECT *
    FROM "Road Intersections" i
    JOIN unnest({}::int[])
    WITH ORDINALITY t(intersection_index, ord)
    USING (intersection_index)
    ORDER BY t.ord
    )
    SELECT degrees(ST_Azimuth(ST_Transform(i.geometry, 26915), ST_Transform(p.geometry,26915))), -- https://gis.stackexchange.com/questions/54427/how-to-find-out-direction-postgis
    ST_Distance(ST_Transform(i.geometry, 26915), ST_Transform(p.geometry,26915)),
    i."NS_cross_street",
    i."EW_cross_street", i.ord
    FROM sensor_data p
    INNER JOIN intersection_data i ON (p.ord = i.ord);
    ''').format(sql.Literal(sensor_indices), sql.Literal(intersection_indices))

    cur.execute(cmd) # Execute

    conn.commit() # Committ command

    response = cur.fetchall()
    
    # Now compose the messages
    
    messages = []
    
    for r in response:
    
        degree, distance, ns_cross_street, ew_cross_street, order = r # Unpack the tuple
        
        direction_string = degrees_to_cardinal(degree)
        
        # Long Version (2 segments)

#         message = f'''SPIKE ALERT! 
# PurpleAir Sensor {sensor_indices[order-1]} is reading {readings[order-1]} micrograms/meter^3.
# This sensor is about {round(distance/1609,2)} miles {direction_string} from the intersection of {ew_cross_street} and {ns_cross_street}. 
# Webmap - https://map.purpleair.com/?select={sensor_indices[order-1]}/44.9723/-93.2447

# Please reply with STOP to be removed from SpikeAlerts.'''
        
        # Shorter version (1 segment)
        
        message = f'''SPIKE ALERT!
Air quality is unhealthy in your area
https://map.purpleair.com/?select={sensor_indices[order-1]}/44.9723/-93.2447

Please reply STOP to end these alerts'''

        messages += [message]
        
    return messages


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def end_alert_messages(sensor_indices, durations, max_readings):
    '''
    Get a list of messages to send when an alert is over
    This function returns a list of messages.
    All inputs must be iterables!
    '''
    
    messages = []
    
    for i in len(sensor_indices):
    
        message = f'''Alert for sensor {sensor_index} is over.
Event Duration: {duration} minutes 
Max value: {max_reading} micrograms/meter^3

Please reply with STOP to be removed from SpikeAlerts.'''

        messages += [message]
        
    return messages
