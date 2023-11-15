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


def Flag_station_channel_state(sensor_indices, pg_connection_dict):
    '''
    Sets all channel_states to zero for the sensor_indices (a list of sensor_index/integers) in "PurpleAir Stations"
    '''
    
    conn = psycopg2.connect(**pg_connection_dict)
    
    # Create json cursor
    cur = conn.cursor()
    
    cmd = sql.SQL('''UPDATE "PurpleAir Stations"
SET channel_state = 0
WHERE sensor_index = ANY ( {} );
    ''').format(sql.Literal(sensor_indices))
    
    cur.execute(cmd) # Execute
    
    conn.commit() # Committ command
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
def Add_new_PurpleAir_Stations(sensor_indices, pg_connection_dict, purpleAir_api):
    '''
    blahg
    '''
    
    #Setting parameters for API
    fields = ['firmware_version','date_created','last_modified','last_seen', 'name', 'uptime','position_rating','channel_state','channel_flags','altitude',
                  'latitude', 'longitude']
                  
    fields_string = 'fields=' + '%2C'.join(fields)
    
    sensor_string = 'show_only=' + '%2C'.join([str(sensor_index) for sensor_index in sensor_indices])
    
    query_string = '&'.join([fields_string, sensor_string])
    
    response = getSensorsData(query_string, purpleAir_api)

    # Unpack response
    
    response_dict = response.json() # Read response as a json (dictionary)
    
    col_names = response_dict['fields']
    data = np.array(response_dict['data'])

    df = pd.DataFrame(data, columns = col_names)

    # Correct Last Seen/modified/date created into datetimes (in UTC UNIX time)

    df['last_modified'] = pd.to_datetime(df['last_modified'].astype(int),
                                                 utc = True,
                                                 unit='s').dt.tz_convert('America/Chicago')
    df['date_created'] = pd.to_datetime(df['date_created'].astype(int),
                                             utc = True,
                                             unit='s').dt.tz_convert('America/Chicago')
    df['last_seen'] = pd.to_datetime(df['last_seen'].astype(int),
                                             utc = True,
                                             unit='s').dt.tz_convert('America/Chicago')
    
     # Make sure sensor_index is an integer
    
    df['sensor_index'] = pd.to_numeric(df['sensor_index'])

    # Spatializing
                                         
    gdf = gpd.GeoDataFrame(df, 
                                geometry = gpd.points_from_xy(
                                    df.longitude,
                                    df.latitude,
                                    crs = 'EPSG:4326')
                               )
    
    cols_for_db = ['sensor_index', 'firmware_version', 'date_created', 'last_modified', 'last_seen',
     'name', 'uptime', 'position_rating', 'channel_state', 'channel_flags', 'altitude', 'geometry'] 
    
    # Get values ready for database

    sorted_df = gdf.copy()[cols_for_db[:-1]]  # Drop unneccessary columns & sort columns by cols_for db (without geometry - see next line)
    
    # Get Well Known Text of the geometry
                         
    sorted_df['wkt'] = gdf.geometry.apply(lambda x: x.wkt)
    
    # Format the times
    
    sorted_df['date_created'] = gdf.date_created.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
    sorted_df['last_modified'] = gdf.last_modified.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
    sorted_df['last_seen'] = gdf.last_seen.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))

    # Connect to PostGIS Database

    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()
    
    # iterate over the dataframe and insert each row into the database using a SQL INSERT statement
    
    for index, row in sorted_df.copy().iterrows():
    
        q1 = sql.SQL('INSERT INTO "PurpleAir Stations" ({}) VALUES ({},{});').format(
         sql.SQL(', ').join(map(sql.Identifier, cols_for_db)),
         sql.SQL(', ').join(sql.Placeholder() * (len(cols_for_db)-1)),
         sql.SQL('ST_SetSRID(ST_GeomFromText(%s), 4326)::geometry'))
        # print(q1.as_string(conn))
        # print(row)
        # break
        
        cur.execute(q1.as_string(conn),
            (list(row.values))
            )
    # Commit commands
    
    conn.commit()
    
    # Close the cursor and connection
    cur.close()
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

    # TWILIO - See twilio_functions.py
#    phone_numbers_to_unsubscribe = Something from REDCap
#    delete_twilio_info(phone_numbers_to_unsubscribe, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
