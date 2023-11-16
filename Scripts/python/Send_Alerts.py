### Import Packages

# File Manipulation

import os # For working with Operating System
import sys # System arguments
from io import StringIO
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

load_dotenv()

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

def Users_to_message_end_alert(pg_connection_dict, ended_alert_indices):
    '''
    This function will return a list of record_ids from "Sign Up Information" that are subscribed, have empty active_alerts, non-empty cached_alerts, and cached_alerts intersect ended_alert_indices = empty (giving a 10 minute buffer before ending alerts - this can certainly change!)
    
    ended_alert_indices = a list of alert_ids that just ended
    
    returns record_ids_to_text (a list)
    '''
    
    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()

    cmd = sql.SQL('''
    SELECT record_id
    FROM "Sign Up Information"
    WHERE subscribed = TRUE
        AND active_alerts = {}
    	AND ARRAY_LENGTH(cached_alerts, 1) > 0 
    	AND NOT cached_alerts && {}::bigint[];
    ''').format(sql.Literal('{}'),
      sql.Literal(ended_alert_indices))

    cur.execute(cmd)

    conn.commit()

    record_ids_to_text = [i[0] for i in cur.fetchall()]

    # Close cursor
    cur.close()
    # Close connection
    conn.close() 

    return record_ids_to_text
    
# ~~~~~~~~~~~~~~ 
def initialize_report(record_id, reports_for_day, pg_connection_dict):
    '''
    This function will initialize a unique report for a user in the database.

    It will also return the duration_minutes/max_reading/report_id of the report
    '''
    
    # Create Report_id
    
    report_id = str(reports_for_day).zfill(5) + '-' + now.strftime('%m%d%y') # XXXXX-MMDDYY
    
    # Create Cursor for commands
    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()

    # Use the record_id to query for the user's cached_alerts
    # Then aggregate from those alerts the start_time, time_difference, max_reading, and nested sensor_indices
    # Unnest the sensor indices into an array of unique sensor_indices
    # Lastly, it will insert all the information into "Reports Archive"
    
    cmd = sql.SQL('''WITH alert_cache as
(
	SELECT cached_alerts
	FROM "Sign Up Information"
	WHERE record_id = {} --inserted record_id
), alerts as
(
	SELECT MIN(p.start_time) as start_time,
			CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago' 
				- MIN(p.start_time) as time_diff,
			MAX(p.max_reading) as max_reading, 
			ARRAY_AGG(p.sensor_indices) as sensor_indices
	FROM "Archived Alerts Acute PurpleAir" p, alert_cache c
	WHERE p.alert_index = ANY (c.cached_alerts)
), unnested_sensors as 
(
	SELECT ARRAY_AGG(DISTINCT x.v) as sensor_indices
	FROM alerts cross JOIN LATERAL unnest(alerts.sensor_indices) as x(v)
)
INSERT INTO "Reports Archive"
SELECT {}, -- Inserted report_id
        a.start_time, -- start_time
		(((DATE_PART('day', a.time_diff) * 24) + 
    		DATE_PART('hour', a.time_diff)) * 60 + 
		 	DATE_PART('minute', a.time_diff)) as duration_minutes,
			a.max_reading, -- max_reading
		n.sensor_indices,
		c.cached_alerts
FROM alert_cache c, alerts a, unnested_sensors n;
''').format(sql.Literal(record_id),
            sql.Literal(report_id))

    cur.execute(cmd)
    # Commit command
    conn.commit()

    # Now get the information from that report

    cmd = sql.SQL('''SELECT duration_minutes, max_reading
             FROM "Reports Archive"
             WHERE report_id = {};
''').format(sql.Literal(report_id))

    cur.execute(cmd)
    # Commit command
    conn.commit()

    # Unpack response
    duration_minutes, max_reading = cur.fetchall()[0]
    # Close cursor
    cur.close()
    # Close connection
    conn.close()

    return duration_minutes, max_reading, report_id
  
# ~~~~~~~~~~~~~~ 
   
def send_all_messages(record_ids, messages, redCap_token_signUp, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_NUMBER, pg_connection_dict):
    '''
    This function will
    1. Send each message to the corresponding record_id
    2. update the user signup data to reflect each new message sent (+1 messages_sent, time added)

    Assumptions: 
    - We won't message the same user twice within an invocation of this function. Otherwise we might need to aggregate the data before step #2
    '''
    
    #import twilio_functions # This didn't work with my version yet, leaving for future reference
    
    numbers = get_phone_numbers(record_ids, redCap_token_signUp) # See Send_Alerts.py
    
    # Check Unsubscriptions
    
    unsubscribed_indices = check_unsubscriptions(numbers, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) # See twilio_functions.py
    
    if len(unsubscribed_indices) > 0:
    
        # Unsubscribe from our database - see Send_Alerts.py
        record_ids_to_unsubscribe = list(np.array(record_ids)[unsubscribed_indices])
        Unsubscribe_users(record_ids_to_unsubscribe, pg_connection_dict)
        # Delete Twilio Information - see twilio_functions.py
        numbers_to_unsubscribe = list(np.array(numbers)[unsubscribed_indices])
        delete_twilio_info(numbers_to_unsubscribe, account_sid, auth_token)
        
        # pop() unsubscriptions from numbers/record_ids/messages list
        
        for unsubscribed_index in unsubscribed_indices:
            
            numbers.pop(unsubscribed_index)
            record_ids.pop(unsubscribed_index)
            messages.pop(unsubscribed_index)
        
    # Send messages
    
    times = send_texts(numbers, messages, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_NUMBER) # See twilio_functions.py
    
    # This didn't work with my version yet, leaving for future reference
    #times = twilio_functions.send_texts(numbers, messages) # this will send all the texts
    
    update_user_table(record_ids, times, pg_connection_dict) # See Send_Alerts.py

    return

# ~~~~~~~~~~~~~

def get_phone_numbers(record_ids, redCap_token_signUp):
    '''
    takes a list of record_ids [int] and gets associated phone numbers. 
    
    Returns phone_numbers in a list with the same order as record_ids
    
    ** Right now, phone numbers come in the format '(XXX) XXX-XXXX'
    
    Assumption: there will always be a valid phone number in REDcap data. Otherwise we would need to add error handling in send_all_messages
    '''
    # Initialize return value
    
    phone_numbers = []
    
    ## Prep the REDCap Query
    
    # For how to do this - https://education.arcus.chop.edu/redcap-api/
    # Redcap logic guide - https://cctsi.cuanschutz.edu/docs/librariesprovider28/redcap/redcap-logic-guide.pdf?sfvrsn=258e94ba_2
    # Info from PyCap - https://redcap-tools.github.io/PyCap/api_reference/records/
    
    # Select by record_id <- probably not the best way, but I couldn't get 'record' to work properly in data
    
    record_id_strs = [str(record_id) for record_id in record_ids]
    filterLogic_str = '[record_id]=' + ' OR [record_id]='.join(record_id_strs)
    
    # Select fields
    
    field_names = 'record_id, phone' # Field Names
    
    data = {
    'token': redCap_token_signUp,
    'content': 'record',
    'fields' : field_names,
    'action': 'export',
    'format': 'csv',
    'type': 'flat',
    'csvDelimiter': '',
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'csv',
    'filterLogic': filterLogic_str  
    }
    
    # Send the request
    r = requests.post('https://redcap.ahc.umn.edu/api/',data=data)
    
    # Unpack the response
    
    if r.status_code == 200 and r.text != '\n':
        
        df = pd.read_csv(StringIO(r.text)) # Read as a dataframe
        
        sorted_df = df.set_index('record_id').loc[record_ids] # Sort df by input record_ids
        
        phone_numbers = list(sorted_df.phone) # Extract phone numbers <- currently in format '(XXX) XXX-XXXX'
        
    else:
        print('Error Receiving REDCap data')
    
    return phone_numbers

# ~~~~~~~~~~~~~

def update_user_table(record_ids, times, pg_connection_dict):
    '''
    Takes a list of users + time objects and updates the "Sign Up Information" table
    to increment each user's messages_sent and last_messaged
    '''
    #print("updating Sign Up Information", record_ids, times)

    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()

    # dataframe is sorted by record ID because SQL messages_sent query needs to be ordered (and this needs to match
    sorted = pd.DataFrame({'record_ids': record_ids,'times': times}).sort_values(by = "record_ids")  

    cmd = sql.SQL('''
    SELECT messages_sent
    FROM "Sign Up Information" u
    WHERE u.record_id = ANY ( {} )
    ORDER BY u.record_id asc; 
    ''').format(sql.Literal(record_ids))

    cur.execute(cmd)
    conn.commit()

    messages_sent_list = [i[0] for i in cur.fetchall()] # Unpack results into list
    messages_sent_new = [v+1 for v in messages_sent_list]
    sorted["messages_sent_new"] = messages_sent_new

    # SQL statement that updates each record (identified by record_ids) with new times, messages_sent_new values
    # if this ever has performance trouble, we could try https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
    # which would require record_id to be made into a foreign key  
    for id, time, msg_inc in zip(sorted["record_ids"], sorted["times"], sorted["messages_sent_new"]):
        cmd = sql.SQL('''
        UPDATE "Sign Up Information"
        SET last_messaged = {lm}, messages_sent = {ms} 
        WHERE record_id =  {ri} ;
        ''').format(ri = sql.Literal(id),
                    lm = sql.Literal(time),
                    ms = sql.Literal(msg_inc)
                    )
        cur.execute(cmd)
    
    conn.commit()
    
    cur.close()
    conn.close()
    return
    
# ~~~~~~~~~~~~~
    
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
