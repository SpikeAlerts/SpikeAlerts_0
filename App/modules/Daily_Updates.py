### Import Packages

from dotenv import load_dotenv
import os

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 

from App.modules import Basic_PSQL as psql
from App.modules import Our_Queries as query
from psycopg2 import sql
import psycopg2

# Analysis

import pandas as pd
import geopandas as gpd
import numpy as np

# Load our functions

from App.modules import PurpleAir_Functions as purp
from App.modules import REDCap_Functions as redcap
from App.modules import Twilio_Functions as our_twilio

# Messaging

from App.modules import Create_messages
from App.modules import PurpleAir_Functions as purp
from App.modules import REDCap_Functions as redcap

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Workflow

def workflow(next_update_time, reports_for_day, messages_sent_today, purpleAir_api, redCap_token_signUp, pg_connection_dict, timezone = 'America/Chicago'):
    '''
    This is the full workflow for Daily Updates
    
    returns the next_update_time (datetime timestamp), reports_for_day, messages_sent_today (ints)
    '''
    
    # PurpleAir
    # If we haven't already updated the full sensor list today, let's do that
    
    last_PurpleAir_update = query.Get_last_PurpleAir_update(pg_connection_dict, timezone = timezone) # See Daily_Updates.py      
      
    if last_PurpleAir_update < next_update_time: # If haven't updated full system today
        # Update "PurpleAir Stations" from PurpleAir
        Sensor_Information_Daily_Update(pg_connection_dict, purpleAir_api)
    
        # Update "Sign Up Information" from REDCap - See Daily_Updates.py
        max_record_id = query.Get_newest_user(pg_connection_dict)
        REDCap_df = redcap.Get_new_users(max_record_id, redCap_token_signUp)
        Add_new_users(REDCap_df, pg_connection_dict)
        print(len(REDCap_df), 'new users')
        
        print(reports_for_day, 'reports yesterday')
        print(messages_sent_today, 'messages sent yesterday')
        
        # Initialize storage for daily metrics
        reports_for_day = 0
        messages_sent_today = 0
    
    # Get next update time (in 1 day)
    next_update_time += dt.timedelta(days=1)
    
    return next_update_time, reports_for_day, messages_sent_today

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
    sensors_df = query.Get_our_sensor_info(pg_connection_dict) # Get our sensor info
    
    # Load information from PurpleAir
    nwlng, selat, selng, nwlat = query.Get_extent(pg_connection_dict) # Get bounds of our project
    purpleAir_df = Get_PurpleAir(nwlng, selat, selng, nwlat, purpleAir_api) # Get PurpleAir data    
    
    # Merge the datasets
    merged_df = pd.merge(sensors_df,
                     purpleAir_df, 
                     on = 'sensor_index', 
                     how = 'outer',
                     suffixes = ('_SpikeAlerts',
                                 '_PurpleAir') 
                                 )
                                 
    # Clean up datatypes
    merged_df['sensor_index'] = merged_df.sensor_index.astype(int)
    merged_df['channel_state'] = merged_df.channel_state.astype("Int64")
    merged_df['channel_flags_PurpleAir'] = merged_df.channel_flags_PurpleAir.astype("Int64")
    merged_df['channel_flags_SpikeAlerts'] = merged_df.channel_flags_SpikeAlerts.astype("Int64")
    
    # Sort the sensors
    sensors_dict = Sort_Sensors(merged_df) # A dictionary of lists of sensor_indices - categories/keys: 'Same Names', 'New', 'Expired', 'Conflicting Names', 'New Flags'
    
    if len(sensors_dict['New']): # Add new sensors to our database (another PurpleAir api call)
    
        Add_new_PurpleAir_Stations(sensors_dict['New'], pg_connection_dict, purpleAir_api)
        
    if len(sensors_dict['Expired']): # "Retire" old sensors
        
        Flag_channel_states(sensors_dict['Expired'], pg_connection_dict)
    
    if len(sensors_dict['Conflicting Names']): # Update our name
        
        Update_name(sensors_dict['Conflicting Names'], merged_df, pg_connection_dict)
        
    if len(sensors_dict['New Flags']): # Email the City about these new issues

        Email_City_flagged_sensors(sensors_dict['New Flags'], merged_df)
    
    if len(sensors_dict['Same Names']): # Update our database's last_seen, channel_flags, 
        
        Update_Flags_LastSeen(sensors_dict['Same Names'], merged_df, pg_connection_dict)   
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_PurpleAir(nwlng, selat, selng, nwlat, purpleAir_api):
    '''
    This function gets Purple Air data for all sensors in the given boundary and filters for only City sensors
    
    returns a dataframe purpleAir_df
    fields: 'sensor_index', 'channel_flags', 'last_seen', 'name'
    datatypes: int, int, datetime timezone 'America/ Chicago', str
    '''
    
    fields = ['sensor_index', 'channel_flags', 'last_seen', 'name'] # The PurpleAir fields we want
    
    df, runtime = purp.Get_PurpleAir_df_bounds(fields, nwlng, selat, selng, nwlat, purpleAir_api)
    
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

def Sort_Sensors(merged_df):
    '''
    This function will sort the sensors for daily updates and return a dictionary
    of lists of sensor_indices (integers)
    
    categories/keys: 'Same Names', 'New', 'Expired', 'Conflicting Names', 'New Flags'
    '''
    
    # Conditions (Boolean Pandas Series)
    
    # Do the names match up?
    names_match = (merged_df.name_SpikeAlerts == merged_df.name_PurpleAir)
    # Do we not have the name?
    no_name_SpikeAlerts = (merged_df.name_SpikeAlerts.isna())
    # Does PurpleAir not have the name?
    no_name_PurpleAir = (merged_df.name_PurpleAir.isna())
    # We haven't seen recently? - within 30 days
    not_seen_recently = (merged_df.last_seen_SpikeAlerts <
                            np.datetime64((dt.datetime.now(pytz.timezone('America/Chicago')
                            ) - dt.timedelta(days = 30))))
    # Good channel State
    good_channel_state = (merged_df.channel_state != 0)
    # New Flags (within past day) - a 4 in our database
    is_new_issue = (merged_df.channel_flags_SpikeAlerts == 4)

    # Use the conditions to sort

    same_name_indices = merged_df[names_match].sensor_index.to_list()
    new_indices = merged_df[(~names_match) 
                            & (no_name_SpikeAlerts)].sensor_index.to_list()
    expired_indices = merged_df[(~names_match) 
                                & (no_name_PurpleAir) 
                                & (not_seen_recently)
                                & (good_channel_state)].sensor_index.to_list()
    confilcting_name_indices = merged_df[(~names_match) 
                                        & (~no_name_PurpleAir) 
                                        & (~no_name_SpikeAlerts)].sensor_index.to_list()
    new_flag_indices = merged_df[(is_new_issue)].sensor_index.to_list()
    
    # Create the dictionary
    
    sensors_dict = {'Same Names':same_name_indices,
                   'New':new_indices,
                   'Expired':expired_indices, 
                   'Conflicting Names':confilcting_name_indices, 
                   'New Flags':new_flag_indices
                    }
                    
    return sensors_dict

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
def Add_new_PurpleAir_Stations(sensor_indices, pg_connection_dict, purpleAir_api):
    '''
    This function takes in a list of sensor_indices,
     queries PurpleAir for all of the fields,
    and adds them to our database.
    '''
    
    #Setting parameters for API
    fields = ['date_created', 'last_seen', 'name', 'position_rating','channel_state','channel_flags','altitude',
                  'latitude', 'longitude']          
    
    df, runtime = purp.Get_PurpleAir_df_sensors(purpleAir_api, sensor_indices, fields)

    if len(df) > 0:

        # Correct Last Seen/date created into datetimes (in UTC UNIX time)

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
        
        cols_for_db = ['sensor_index', 'date_created', 'last_seen',
         'name', 'position_rating', 'channel_state', 'channel_flags', 'altitude']
         
        sorted_df = gdf.copy()[cols_for_db] 
        
        # Get Well Known Text of the geometry
                             
        sorted_df['geometry'] = gdf.geometry.apply(lambda x: x.wkt)
        
        # Format the times
        
        sorted_df['date_created'] = gdf.date_created.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
        sorted_df['last_seen'] = gdf.last_seen.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
         
        psql.insert_into(sorted_df, "PurpleAir Stations", pg_connection_dict, is_spatial = True)    
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
def Flag_channel_states(sensor_indices, pg_connection_dict):
    '''
    To be used on sensors that haven't been seen in a while...
    
    Sets all channel_states to zero and channel_flags to 3 for the sensor_indices (a list of sensor_index/integers) in "PurpleAir Stations"
    '''
    
    cmd = sql.SQL('''UPDATE "PurpleAir Stations"
SET channel_state = 0, channel_flags = 3
WHERE sensor_index = ANY ( {} );
    ''').format(sql.Literal(sensor_indices))
    
    psql.send_update(cmd, pg_connection_dict)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Update_name(sensor_indices, merged_df, pg_connection_dict):
    '''
    This function takes in a dataframe of merged "Sensor Information" where the names don't match
    and updates name, last_seen & channel_flags in our database to the most recent PurpleAir data
    
    Left is our database (_SpikeAlerts) and right is the most recent PurpleAir api pull (_PurpleAir)
    
    pd.merge(sensors_df, purpleAir_df, on = 'sensor_index', how = 'outer', suffixes = ('_SpikeAlerts', '_PurpleAir') )
    '''
    
    name_controversy_df = merged_df[merged_df.sensor_index.isin(sensor_indices)].copy()
    
    # Make sure the datatypes are correct
    name_controversy_df['last_seen_PurpleAir'] = name_controversy_df.last_seen_PurpleAir.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
    
    # Connect to database
    conn = psycopg2.connect(**pg_connection_dict,
                            keepalives_idle=20)
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

def Email_City_flagged_sensors(sensor_indices, merged_df):
    '''
    This function composes an email to the city about recently flagged sensors and prints it
    '''
    
    new_issue_df = merged_df[merged_df.sensor_index.isin(sensor_indices)].copy()
    
    # Conditions

    conditions = ['wifi_down?', 'a_down', 'b_down', 'both_down'] # corresponds to 0, 1, 2, 3 from PurpleAir channel_flags

    # Initialize storage

    email = '''Hello City of Minneapolis Health Department,

    Writing today to inform you of some anomalies in the PurpleAir sensors that we discovered:

    name, last seen, channel issue

    '''

    for i, condition in enumerate(conditions):

        con_df = new_issue_df[new_issue_df.channel_flags_PurpleAir == i]
        
        if i == 0: # These wifi issues are only important if older than 6 hours
            not_seen_recently_PurpleAir = (con_df.last_seen_PurpleAir < dt.datetime.now(pytz.timezone('America/Chicago')) - dt.timedelta(hours = 6))
            
            con_df = con_df[not_seen_recently_PurpleAir]
        
        for i, row in con_df.iterrows():
                
            email += f'\n{row.name_PurpleAir}, {row.last_seen_PurpleAir.strftime("%m/%d/%y - %H:%M")}, {condition}'

    email += '\n\nTake Care,\nSpikeAlerts'
    print(email)
        
        
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def Update_Flags_LastSeen(sensor_indices, merged_df, pg_connection_dict):
    '''
    This function takes in a dataframe of merged "Sensor Information" where the names match
    and updates channel_flags & last_seen in our database to the most recent PurpleAir data
    
    Left is our database (_SpikeAlerts) and right is the most recent PurpleAir api pull (_PurpleAir)
    
    pd.merge(sensors_df, purpleAir_df, on = 'sensor_index', how = 'outer', suffixes = ('_SpikeAlerts', '_PurpleAir') )
    '''
    
    sameName_df = merged_df[merged_df.sensor_index.isin(sensor_indices)].copy()
    
    # Make sure sensor_index/channel_flags/last_seen are formatted correct
    
    #sameName_df['sensor_index'] = sameName_df.sensor_index.astype(int)
    #sameName_df['channel_flags_PurpleAir'] = sameName_df.channel_flags_PurpleAir.astype(int)
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

def Add_new_users(df, pg_connection_dict):
    '''
    This function inserts the new users from REDCap into our database
    The dataframe must have "phone", "record_id" and "wkt" as columns with the Well Known Text in WGS84 (EPSG:4326) "Lat/lon"
    '''
    
    if len(df) > 0:
        
        load_dotenv()

        account_sid = os.environ['TWILIO_ACCOUNT_SID']
        auth_token = os.environ['TWILIO_AUTH_TOKEN']
        twilio_number = os.environ['TWILIO_NUMBER']
        # Insert into database
        
        df['geometry'] = df.wkt
        
        #print(df.geometry[0])
        #print(type(df))
        
        df_for_db = df[['record_id', 'geometry']]
        
        psql.insert_into(df_for_db, "Sign Up Information", pg_connection_dict, is_spatial = True)
        
        # Now message those new users
        
        numbers = df.phone.to_list()
        messages = [Create_messages.welcome_message()]*len(numbers)
        
        our_twilio.send_texts(numbers, messages, account_sid, auth_token, twilio_number)
    
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
