### Import Packages

# File manipulation

import os # For working with Operating System
import shutil # For deleting folders
import urllib # For accessing websites
import zipfile # For extracting from Zipfiles
from io import BytesIO # For reading bytes objects

import requests # Accessing the Web
import datetime as dt # Working with dates/times

# Analysis

import numpy as np
import pandas as pd # Data Management
import geopandas as gpd # Spatial Data Manipulation

# Database 

import psycopg2 # For working with postgresql database
from psycopg2 import sql

# Environmental variables

cwd = os.getcwd() 
from dotenv import load_dotenv # Loading .env info
load_dotenv() # Load .env file
purpleAir_api = os.getenv('PURPLEAIR_API_TOKEN')
creds = [os.getenv('DB_NAME'),
         os.getenv('DB_USER'),
         os.getenv('DB_PASS'),
         os.getenv('DB_PORT'),
         os.getenv('DB_HOST')
        ]
pg_connection_dict = dict(zip(['dbname', 'user', 'password', 'port', 'host'], creds)) 

# Load our Functions

script_path = os.path.join('Scripts', 'python')

# Function definition - Please see Scripts/python/*
exec(open(os.path.join(script_path, 'Get_spikes_df.py')).read())
exec(open(os.path.join(script_path, 'Daily_Updates.py')).read())
# exec(open(os.path.join(script_path, 'Update_Alerts.py')).read())

### Definitions

def extract_zip_from_url(url=None, savepath=None):
  # '''Extract a zipfile from the internet
  # then unpack it in to it's own folder 
  # within the working directory.
  # Takes a single url (string).'''

    if not os.path.exists(savepath):
        os.makedirs(savepath)
    # Unload zip into the new folder
    response = urllib.request.urlopen(url) # Get a response
    zip_folder = zipfile.ZipFile(BytesIO(response.read())) # Read Response
    zip_folder.extractall(path=savepath) # Extract files
    zip_folder.close() # Close zip object

def get_boundary_from_url():
  # Download Data
  ## Twin Cities Metro Boundaries - Downloaded from MN GeospatialCommons gisdata.mn.gov  (~ 5mb)
  url = "https://resources.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_metc/bdry_census2020counties_ctus/shp_bdry_census2020counties_ctus.zip"
  # Create folder name for file
  folder_name = 'shp_bdry_census2020counties_ctus' # url.split('/')[-1][:-4] <- programatic way to get foldernam
  # Define path for downloaded files
  savepath = os.path.join(cwd, 'Data', folder_name)
  extract_zip_from_url(url, savepath)

  # Read & Select
  # Get path
  filename = 'Census2020CTUs.shp'
  path = os.path.join(savepath, filename)
  ctus_boundaries = gpd.read_file(path)
  # Select Minneapolis
  mpls_boundary = ctus_boundaries[ctus_boundaries['CTU_NAME'] == 'Minneapolis']
  return mpls_boundary
  # # Write the selected features to a new featureclass
  # arcpy.management.CopyFeatures(mpls_boundary, "mpls_boundary")

def pg_post_boundaries(mpls_boundary):
  # connect to db
  conn = psycopg2.connect(**pg_connection_dict)
  cur = conn.cursor()   # Create Cursor for commands
  ## Redo everything below here
  # Insert into table
  cols = ['CTU_ID', 'CTU_NAME', 'CTU_CODE', 'geometry'] # Relative columns
  for i, row in mpls_boundary[cols].iterrows():
    cur.execute(
      'INSERT INTO "Minneapolis Boundary"("CTU_ID", "CTU_NAME", "CTU_CODE", geometry)'
      'VALUES (%(ctu_id)s, %(ctu_name)s, %(ctu_code)s, ST_Transform(ST_SetSRID(ST_GeomFromText(%(geom)s), 26915),4326)::geometry);',
      {'ctu_id': row.iloc[0],
        'ctu_name' : row.iloc[1],
        'ctu_code': row.iloc[2],
        'geom': row.iloc[3].wkt})
    conn.commit() # Commit command
  cur.close() # Close cursor
  conn.close() # Close connection
  print('initted!')

def pg_get_boundary():
  # Connect to db
  conn = psycopg2.connect(**pg_connection_dict) 
  cur = conn.cursor() # Create cursor
  cur.execute('''
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
  conn.commit() # Commit command
  response = cur.fetchall()[0][0]   # Gives a string
  cur.close() # Close cursor
  conn.close() # Close connection

  # Unpack the response
  num_string = response[4:-1]
  xmin = num_string.split(' ')[0]
  ymin = num_string.split(' ')[1].split(',')[0]
  xmax = num_string.split(' ')[1].split(',')[1]
  ymax = num_string.split(' ')[2]
  return (xmin, ymin, xmax, ymax)

def import_sensors_data(boundaries):
  ## Imports sensor

  #Set bounding strings for API parameters
  nwlng, selat, selng, nwlat = boundaries # Convert into PurpleAir API notation
  bounds_strings = [f'nwlng={nwlng}',
                    f'nwlat={nwlat}',
                    f'selng={selng}',
                    f'selat={selat}']
  bounds_string = '&'.join(bounds_strings)

  #Setting parameters for API
  # Fields
  fields = ['firmware_version','date_created','last_modified','last_seen',
            'name', 'uptime','position_rating','channel_state','channel_flags',
            'altitude', 'latitude', 'longitude', 'location_type']
  fields_string = 'fields=' + '%2C'.join(fields)
  # Finalizing query for API function
  query_string = '&'.join([fields_string, bounds_string])
  #calling the API
  return getSensorsData(query_string, purpleAir_api) # See Get_spikes_df.py

def format_sensor_data(response):
  # Unpack response
  response_dict = response.json() # Read response as a json (dictionary)
  col_names = response_dict['fields']
  data = np.array(response_dict['data'])
  df = pd.DataFrame(data, columns = col_names) # Convert to dataframe

  # Correct Last Seen/modified/date created into datetimes
  df['last_modified'] = pd.to_datetime(df['last_modified'].astype(int),
                                              utc = True,
                                              unit='s').dt.tz_convert('America/Chicago')
  df['date_created'] = pd.to_datetime(df['date_created'].astype(int),
                                          utc = True,
                                          unit='s').dt.tz_convert('America/Chicago')
  df['last_seen'] = pd.to_datetime(df['last_seen'].astype(int),
                                          utc = True,
                                          unit='s').dt.tz_convert('America/Chicago')

  # Make sure sensor_index/location_type is an integer
  df['sensor_index'] = pd.to_numeric(df['sensor_index'])
  df['location_type'] = pd.to_numeric(df['location_type'])

  # Filter for City of Minneapolis & outside sensors
  is_city = df.name.apply(lambda x: 'CITY OF MINNEAPOLIS' in x.upper())
  is_outside = df.location_type == 0
  purpleAir_df = df[is_city & is_outside].copy()
  gdf = gpd.GeoDataFrame(purpleAir_df, 
                            geometry = gpd.points_from_xy(
                                purpleAir_df.longitude,
                                purpleAir_df.latitude,
                                crs = 'EPSG:4326')
                          )
  
  #format for database
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

  return (cols_for_db, sorted_df)

def pg_post_init_sensor_data(cols_for_db, sorted_df):
  # Insert into database
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

def db_init():
  # execute get boundary --> post values to database
  pg_post_boundaries(get_boundary_from_url())

  boundaries = pg_get_boundary() # get boundaries from db
  print(boundaries)
  response = import_sensors_data(boundaries) # import sensors data from purpleair api
  print(response)
  cols_for_db, sorted_df = format_sensor_data(response) # format sensors data. returns column names and formated geo dataframe
  print(cols_for_db, sorted_df)
  pg_post_init_sensor_data(cols_for_db, sorted_df)

def db_notinit():
   print("you're not init!")