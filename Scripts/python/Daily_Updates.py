### Import Packages

# File manipulation

import os # For working with Operating System
import requests # Accessing the Web
from io import StringIO
import datetime as dt # Working with dates/times

# Database 

import psycopg2
from psycopg2 import sql

# Analysis

import numpy as np
import pandas as pd

# Load our functions

# import Get_spikes_df as get_spikes
exec(open('Get_spikes_df.py').read())

# If in notebooks... 
# exec(open(os.path.join(script_path, 'Get_spikes_df.py')).read())

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# PurpleAir Stations

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~      

def Get_last_PurpleAir_update(pg_connection_dict, timezone = 'America/Chicago'):
    '''
    This function gets the highest last_seen (only updated daily)
    
    returns timezone aware datetime
    '''
    # Connect
    conn = psycopg2.connect(**pg_connection_dict) 
    # Create cursor
    cur = conn.cursor()

    cmd = sql.SQL('''SELECT MAX(last_seen)
    FROM "PurpleAir Stations"
    WHERE channel_flags = 0;
    ''')

    cur.execute(cmd) # Execute
    conn.commit() # Committ command

    # Unpack response into timezone aware datetime

    time = cur.fetchall()[0][0].replace(tzinfo=pytz.timezone(timezone))

    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
    return time
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~      

def Sensor_Information_Daily_Update(pg_connection_dict, purpleAir_api):
    '''
    This is the full workflow for updating our sensor information in the database. 
    Please see Daily_Updates.py for specifics on the functions
    
    test with 
    
    UPDATE "PurpleAir Stations"
    SET channel_state = 3, channel_flags = 4;

    DELETE FROM "PurpleAir Stations"
    WHERE sensor_index = 143634;

    UPDATE "PurpleAir Stations"
    SET name = 'wrong_name'
    WHERE sensor_index = 143916;
    '''
    # Load information from our database
    sensors_df = Get_our_sensor_info(pg_connection_dict) # Get our sensor info
    
    # Load information from PurpleAir
    nwlng, selat, selng, nwlat = Get_extent(pg_connection_dict) # Get bounds of our project
    purpleAir_df = Get_PurpleAir(nwlng, selat, selng, nwlat, purpleAir_api) # Get PurpleAir data
    
    # Merge the datasets
    merged = pd.merge(sensors_df,
                     purpleAir_df, 
                     on = 'sensor_index', 
                     how = 'outer',
                     suffixes = ('_SpikeAlerts',
                                 '_PurpleAir') 
                                 )
                                 
    # Clean up datatypes
    merged['sensor_index'] = merged.sensor_index.astype(int)
    merged['channel_state'] = merged.channel_state.astype("Int64")
    
    # Do the names match up?
    names_match = (merged.name_SpikeAlerts == merged.name_PurpleAir)
    
    # Different Names
    diffName_df = merged[~names_match]
    
    if len(diffName_df):
    
        ## New Name - in PurpleAir not ours - Add to our database (another PurpleAir api call)
        ### Conditions
        is_new_name = diffName_df.name_SpikeAlerts.isna() # Boolean Series
        # Sensor Indices as a list
        new_indices = diffName_df[is_new_name].sensor_index.to_list()
        if len(new_indices) > 0:
            Add_new_PurpleAir_Stations(new_indices, pg_connection_dict, purpleAir_api)
        
        ## No PurpleAir Name - Potentially old sensors - flag channel_state if last seen greater than 4 days
        ### Conditions
        no_name_PurpleAir = (diffName_df.name_PurpleAir.isna()) # Boolean Series
        not_seen_recently = (diffName_df.last_seen_SpikeAlerts.dt.date <
                            (dt.datetime.now(pytz.timezone('America/Chicago')
                            ) - dt.timedelta(days = 4)).date()) # Seen recently?
        good_channel_state = (diffName_df.channel_state != 0) # Were we aware?
        # Sensor Indices as a list
        bad_indices = diffName_df[no_name_PurpleAir & not_seen_recently & good_channel_state
                                  ].sensor_index.to_list()
        if len(bad_indices) > 0:
            Flag_station_channel_state(bad_indices, pg_connection_dict)
        
        ## Both have names but they're different - update with purpleair info
        ### Conditions
        name_controversy = (~no_name_PurpleAir & ~is_new_name) # Not new and not no name from PurpleAir
        # The dataframe under these conditions
        name_controversy_df = diffName_df[name_controversy].copy() # Has a different name!
        if len(name_controversy_df.sensor_index) > 0:
            Update_name(name_controversy_df, pg_connection_dict)
            
            
    # Same Names
    
    # If we've got a 4 in our channel_flags
    # the issue is from the previous day.

    # We should probably notify the City! <- done in notebook 3_Daily_Updates/1_PurpleAir_Stations.ipynb

    is_new_issue = (merged.channel_flags_SpikeAlerts == 4)

    new_issue_df = merged[is_new_issue]
    
    if len(new_issue_df) > 0:
    
        # Conditions

        conditions = ['wifi_down?', 'a_down', 'b_down', 'both_down'] # corresponds to 0, 1, 2, 3 from PurpleAir channel_flags

        # Initialize storage

        email = '''Hello City of Minneapolis Health Department,

        Writing today to inform you of some anomalies in the PurpleAir sensors that we discovered:

        name, last seen, channel issue

        '''

        for i, condition in enumerate(conditions):

            if i == 0: # Only "serious" wifi issue if longer than 6 hours

                con_df = new_issue_df[(new_issue_df.channel_flags_PurpleAir == i
                                        ) & (new_issue_df.last_seen_PurpleAir < dt.datetime.now(pytz.timezone('America/Chicago')) - dt.timedelta(hours = 6))]
            
            else:  
                con_df = new_issue_df[new_issue_df.channel_flags_PurpleAir == i]

            for i, row in con_df.iterrows():

                    
                email += f'\n{row.name_PurpleAir}, {row.last_seen_PurpleAir.strftime("%m/%d/%y - %H:%M")}, {condition}'

        email += '\n\nTake Care,\nSpikeAlerts'
        print(email)
        
    # Then update all the channel flags and last seens

    Update_Flags_LastSeen(merged[names_match].copy(), pg_connection_dict)
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_our_sensor_info(pg_connection_dict):
    '''
    Gets all sensors from our database for a daily update check
    
    returns sensors_df 
    
    with columns sensor_index, last_seen, name, channel_flags, channel_state
    types: int, datetime 'America/Chicago', str, int, int
    '''
    
    # Connect
    conn = psycopg2.connect(**pg_connection_dict) 
    # Create cursor
    cur = conn.cursor()

    cmd = sql.SQL('''SELECT sensor_index, last_seen, name, channel_flags, channel_state
    FROM "PurpleAir Stations";
    ''')

    cur.execute(cmd) # Execute
    conn.commit() # Committ command

    # Unpack response into pandas series

    sensors_df = pd.DataFrame(cur.fetchall(), columns = ['sensor_index', 'last_seen', 'name', 'channel_flags', 'channel_state'])
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()

    # Datatype corrections
    sensors_df['sensor_index']  = sensors_df.sensor_index.astype(int)
    sensors_df['last_seen'] = pd.to_datetime(sensors_df['last_seen'])
    sensors_df['channel_flags'] = sensors_df.channel_flags.astype(int)
    sensors_df['channel_state'] = sensors_df.channel_state.astype(int)
    
    return sensors_df
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_extent(pg_connection_dict): 
    '''
    Gets the bounding box of our project's extent + 100 meters ("Minneapolis Boundary")
    
    Specifically for PurpleAir api
    
    returns nwlng, selat, selng, nwlat AS strings
    '''   
    
    # Connect
    conn = psycopg2.connect(**pg_connection_dict) 
    # Create cursor
    cur = conn.cursor()
    
    # Query for bounding box of boundary buffered 100 meters

    cmd = sql.SQL('''
    WITH buffer as
	    (
	    SELECT ST_BUFFER(ST_Transform(ST_SetSRID(geometry, 4326),
								      26915),
					     100) geom -- buff the geometry by 100 meters
	    FROM "Minneapolis Boundary"
	    ), bbox as
	    (
	    SELECT ST_EXTENT(ST_Transform(geom, 4326)) b
	    FROM buffer
	    )
    SELECT b::text
    FROM bbox;
    ''')

    cur.execute(cmd) # Execute
    conn.commit() # Committ command
    
    # Gives a string
    response = cur.fetchall()[0][0]
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
    # Unpack the response

    num_string = response[4:-1]
    
    # That's in xmin, ymin, xmax, ymax
    xmin = num_string.split(' ')[0]
    ymin = num_string.split(' ')[1].split(',')[0]
    xmax = num_string.split(' ')[1].split(',')[1]
    ymax = num_string.split(' ')[2]
    
    # Convert into PurpleAir API notation
    nwlng, selat, selng, nwlat = xmin, ymin, xmax, ymax
    
    return nwlng, selat, selng, nwlat
    
# ~~~~~~~~~~~~~~~~~~~~~~~~

def Get_PurpleAir(nwlng, selat, selng, nwlat, purpleAir_api):
    '''
    This function gets Purple Air data for all sensors in the given boundary
    
    returns a dataframe purpleAir_df
    fields: 'sensor_index', 'channel_flags', 'last_seen', 'name'
    datatypes: int, int, datetime timezone 'America/ Chicago', str
    '''
    
    #Setting parameters for API call for comparing to our data

    # Bounding string
    bounds_strings = [f'nwlng={nwlng}',
                      f'nwlat={nwlat}',
                      f'selng={selng}',
                      f'selat={selat}']
    bounds_string = '&'.join(bounds_strings)  
    # Field string
    fields = ['sensor_index', 'channel_flags', 'last_seen', 'name']
    fields_string = 'fields=' + '%2C'.join(fields)

    # Finalizing query for API function
    query_string = '&'.join([fields_string, bounds_string])
    
    response = getSensorsData(query_string, purpleAir_api) # See Get_spikes_df.py
    
    # Unpack response as a dataframe
    response_dict = response.json() # Read response as a json (dictionary)

    col_names = response_dict['fields']
    data = np.array(response_dict['data'])
    df = pd.DataFrame(data, columns = col_names) # Convert to dataframe

    
    # Datatype corrections
    df['sensor_index']  = df.sensor_index.astype(int)
    df['last_seen'] = pd.to_datetime(df['last_seen'].astype(int),
                                             utc = True,
                                             unit='s').dt.tz_convert('America/Chicago')
    df['channel_flags'] = df.channel_flags.astype(int)


    # Filter for City of Minneapolis
    is_city = df.name.apply(lambda x: 'CITY OF MINNEAPOLIS' in x.upper())

    purpleAir_df =  df[is_city].copy()
    
    return purpleAir_df
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
def Add_new_PurpleAir_Stations(sensor_indices, pg_connection_dict, purpleAir_api):
    '''
    This function takes in a list of sensor_indices,
     queries PurpleAir for all of the fields,
    and adds them to our database.
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
    
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
def Flag_channel_state(sensor_indices, pg_connection_dict):
    '''
    To be used on sensors that haven't been seen in a while...
    
    Sets all channel_states to zero and channel_flags to 3 for the sensor_indices (a list of sensor_index/integers) in "PurpleAir Stations"
    '''
    
    conn = psycopg2.connect(**pg_connection_dict)
    
    # Create json cursor
    cur = conn.cursor()
    
    cmd = sql.SQL('''UPDATE "PurpleAir Stations"
SET channel_state = 0, channel_flags = 3
WHERE sensor_index = ANY ( {} );
    ''').format(sql.Literal(sensor_indices))
    
    cur.execute(cmd) # Execute
    
    conn.commit() # Committ command
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Update_name(name_controversy_df, pg_connection_dict):
    '''
    This function takes in a dataframe of merged "Sensor Information" where the names don't match
    and updates name, last_seen & channel_flags in our database to the most recent PurpleAir data
    
    Left is our database (_SpikeAlerts) and right is the most recent PurpleAir api pull (_PurpleAir)
    
    pd.merge(sensors_df, purpleAir_df, on = 'sensor_index', how = 'outer', suffixes = ('_SpikeAlerts', '_PurpleAir') )
    '''
    
    # Make sure the datatypes are correct
    name_controversy_df['channel_flags_PurpleAir'] = name_controversy_df.channel_flags_PurpleAir.astype(int)
    name_controversy_df['last_seen_PurpleAir'] = name_controversy_df.last_seen_PurpleAir.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
    
    # Connect to database
    conn = psycopg2.connect(**pg_connection_dict) 
    # Create cursor
    cur = conn.cursor()
    
    for i, row in name_controversy_df.iterrows():
    
        cmd = sql.SQL('''UPDATE "PurpleAir Stations"
        SET name = {}, last_seen = {}, channel_flags = {}
        WHERE sensor_index = {};
        ''').format(sql.Literal(row.name_PurpleAir),
                    sql.Literal(row.last_seen_PurpleAir),
                    sql.Literal(row.channel_flags_PurpleAir),
                    sql.Literal(row.sensor_index))
        
        cur.execute(cmd)
        
    conn.commit()
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def Update_Flags_LastSeen(sameName_df, pg_connection_dict):
    '''
    This function takes in a dataframe of merged "Sensor Information" where the names match
    and updates channel_flags & last_seen in our database to the most recent PurpleAir data
    
    Left is our database (_SpikeAlerts) and right is the most recent PurpleAir api pull (_PurpleAir)
    
    pd.merge(sensors_df, purpleAir_df, on = 'sensor_index', how = 'outer', suffixes = ('_SpikeAlerts', '_PurpleAir') )
    '''
    
    # Make sure sensor_index/channel_flags/last_seen are formatted correct
    
    sameName_df['sensor_index'] = sameName_df.sensor_index.astype(int)
    sameName_df['channel_flags_PurpleAir'] = sameName_df.channel_flags_PurpleAir.astype(int)
    sameName_df['last_seen_PurpleAir'] = sameName_df.last_seen_PurpleAir.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
    
    if len(sameName_df.sensor_index) > 0: 

        # Connect to database
        conn = psycopg2.connect(**pg_connection_dict) 
        # Create cursor
        cur = conn.cursor()
        
        for i, row in sameName_df.iterrows():
        
            cmd = sql.SQL('''UPDATE "PurpleAir Stations"
            SET last_seen = {}, channel_flags = {}
            WHERE sensor_index = {};
            ''').format(sql.Literal(row.last_seen_PurpleAir),
                        sql.Literal(row.channel_flags_PurpleAir),
                        sql.Literal(row.sensor_index))
            
            cur.execute(cmd)
            
        conn.commit()
        
        # Close cursor
        cur.close()
        # Close connection
        conn.close()
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Sign Up Information

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~      

def Get_newest_user(pg_connection_dict):
    '''
    This function gets the newest user's record_id
    
    returns an integer
    '''
    # Connect
    conn = psycopg2.connect(**pg_connection_dict) 
    # Create cursor
    cur = conn.cursor()

    cmd = sql.SQL('''SELECT MAX(record_id)
    FROM "Sign Up Information";;
    ''')

    cur.execute(cmd) # Execute
    conn.commit() # Committ command

    # Unpack response into pandas series

    max_record_id = cur.fetchall()[0][0]

    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
    return max_record_id
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~      

def Add_new_users_from_REDCap(max_record_id, redCap_token_signUp, pg_connection_dict):
    '''
    This function gets the newest user's record_id
    '''
    
    # REDCap Filter logic
    filterLogic_str = f"[record_id]>{max_record_id}"
    
    # REDCap request
    data = {
    'token': redCap_token_signUp,
    'content': 'record',
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
    
    r = requests.post('https://redcap.ahc.umn.edu/api/',data=data)
    
    # Unpack response
    
    if r.status_code == 200 and r.text != '\n': # If the request worked and non-empty
    
        df = pd.read_csv(StringIO(r.text))
        
        # Spatialize dataframe

        gdf = gpd.GeoDataFrame(df, 
                                geometry = gpd.points_from_xy(
                                            df.lon,
                                            df.lat,
                                            crs = 'EPSG:4326')
                                       )

        gdf['wkt'] = gdf.geometry.apply(lambda x: x.wkt)
        
        # Prep for database 

        focus_df = gdf[['record_id', 'wkt']]
        cols_for_db = ['record_id', 'geometry'] 
        
        # Insert into database
        
        # Connect
        conn = psycopg2.connect(**pg_connection_dict) 
        # Create cursor
        cur = conn.cursor()

        for i, row in focus_df.iterrows():

            q1 = sql.SQL('INSERT INTO "Sign Up Information" ({}) VALUES ({},{});').format(
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

        # Close cursor
        cur.close()
        # Close connection
        conn.close()

        print(len(focus_df), ' new users')
        
    else:
        print('0 new users')
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Subscriptions

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~       
    
    
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
