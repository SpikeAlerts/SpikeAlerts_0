### Import Packages

# File Manipulation

import os # For working with Operating System
import sys # System arguments
from dotenv import load_dotenv # Loading .env info

# Web

import requests # Accessing the Web

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 

import psycopg2
from psycopg2 import sql

# Data Manipulation

import numpy as np
import geopandas as gpd
import pandas as pd

# Functions 

def Users_nearby_sensor(pg_connection_dict, sensor_index, distance):
    '''
    This function will return a list of record_ids from "Sign Up Information that are within the distance from the sensor
    
    sensor_index = integer
    distance = integer (in meters)
    
    returns record_ids (a list)
    '''
    
    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()

    cmd = sql.SQL('''
    WITH sensor as -- query for the desired sensor
    (
    SELECT sensor_index, geometry
    FROM "PurpleAir Stations"
    WHERE sensor_index = {}
    )
    SELECT record_id
    FROM "Sign Up Information" u, sensor s
    WHERE u.subscribed = TRUE AND ST_DWithin(ST_Transform(u.geometry,26915), -- query for users within the distance from the sensor
										    ST_Transform(s.geometry, 26915),{}); 
    ''').format(sql.Literal(sensor_index),
                sql.Literal(distance))

    cur.execute(cmd)

    conn.commit()

    record_ids = [i[0] for i in cur.fetchall()] # Unpack results into list

    # Close cursor
    cur.close()
    # Close connection
    conn.close() 

    return record_ids

