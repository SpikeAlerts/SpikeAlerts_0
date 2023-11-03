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

## Load Functions
import twilio_functions

creds = [os.getenv('DB_NAME'),
         os.getenv('DB_USER'),
         os.getenv('DB_PASS'),
         os.getenv('DB_PORT'),
         os.getenv('DB_HOST')
        ]

pg_connection_dict = dict(zip(['dbname', 'user', 'password', 'port', 'host'], creds)) 

# Functions 

def Users_nearby_sensor(pg_connection_dict, sensor_index, distance):
    '''
    This function will return a list of record_ids from "Sign Up Information" that are within the distance from the sensor
    
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

# ~~~~~~~~~~~~~~

def Users_to_message_new_alert(pg_connection_dict, record_ids):
    '''
    This function will return a list of record_ids from "Sign Up Information" that have empty active and cached alerts and are in the list or record_ids given
    
    record_ids = a list of ids to check
    
    returns record_ids_to_text (a list)
    '''
    
    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()

    cmd = sql.SQL('''
    SELECT record_id
    FROM "Sign Up Information"
    WHERE active_alerts = {} AND cached_alerts = {} AND record_id = ANY ( {} );
    ''').format(sql.Literal('{}'), sql.Literal('{}'), sql.Literal(record_ids))

    cur.execute(cmd)

    conn.commit()

    record_ids_to_text = [i[0] for i in cur.fetchall()]

    # Close cursor
    cur.close()
    # Close connection
    conn.close() 

    return record_ids_to_text
    
# ~~~~~~~~~~~~~~ 
   
def send_all_messages(record_ids, messages):
    '''
    This function will
    1. Send each message to the corresponding record_id
    2. update the user signup data to reflect each new message sent (+1 messages_sent, time added)

    Assumptions: 
    - We won't message the same user twice within an invocation of this function. Otherwise we might need to aggregate the data before step #2
    '''
    numbers = get_phone_numbers(record_ids)
    times = twilio_functions.send_texts(numbers, messages) # this will send all the texts

    update_user_table(record_ids, times)

    return



def get_phone_numbers(users):
    '''
    takes a list of users and gets associated phone numbers
    
    Assumption: there will always be a valid phone number in REDcap data. Otherwise we would need to add error handling in send_all_messages
    '''

    print("getting numbers!")
    
    numbers = []
    for record_id in users:
        ### needs some contact with REDCap. API? 
        # per ERD shouldn't have any interaction w/ Sign Up Info
        numbers.append(int(record_id*100))
    
    return numbers

def update_user_table(record_ids, times):
    '''
    Takes a list of users + time objects and updates the "Sign Up Information" table
    to increment each user's messages_sent and last_messaged
    '''
    print("updating Sign Up Information")

    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()

    cmd = sql.SQL('''
    SELECT messages_sent
    FROM "Sign Up Information" u
    WHERE u.record_id = ANY ( {} ); 
    ''').format(sql.Literal(record_ids))

    cur.execute(cmd)

    conn.commit()

    messages_sent_list = [i[0] for i in cur.fetchall()] # Unpack results into list
    messages_sent_new = [v+1 for v in messages_sent_list]

    print(record_ids, times, messages_sent_new)
    #2. SQL statement that updates each record (identified by record_ids) with new times, messages_sent_new values
    print("fancy SQL to update the sign up table still pending")

    # Close cursor
    cur.close()
    # Close connection
    conn.close() 
    return