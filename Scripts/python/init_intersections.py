import os # For working with Operating System
import urllib # For accessing websites
import zipfile # For extracting from Zipfiles
from io import BytesIO # For reading bytes objects
import psycopg2
from psycopg2 import sql, extras
import numpy as np # For working with Arrays
import pandas as pd # Data Manipulation
import geopandas as gpd

def get_linetrend(geom):
    '''This looks at the first and last points in a UTM geometry linestring 
    and says whether it trends more North South (NS) or East West (EW)'''
    coords = list(geom.coords)
    first_coord = coords[0]
    last_coord = coords[-1]

    ns_trend = abs(first_coord[1] - last_coord[1])
    ew_trend = abs(first_coord[0] - last_coord[0])

    if ns_trend > ew_trend:
        result = 'NS'
    else:
        result = 'EW'
    return result

def Get_nearby_sensors(target_geom, sensors_df, distance):
    ''' This function will return a list of all sensor indices within the 
    distance (meters) of the target'''
    sensors_geo = sensors_df.geometry # Get geometries of targets and sources 
    target_buffer = target_geom.buffer(distance)
    is_within = sensors_geo.within(target_buffer)
    return pd.to_numeric(sensors_df[is_within].sensor_index).tolist()

cwd = os.getcwd() # This is a global variable for where the notebook is (must change if running in arcpro)
datapath = os.path.join('..', '..', 'Data')
cred_pth = os.path.join(os.getcwd(), '..', '..', 'Scripts', 'database', 'db_credentials.txt')
with open(cred_pth, 'r') as f:
    creds = f.readlines()[0].rstrip('\n').split(', ') # creds to connect to PostGIS db
pg_connection_dict = dict(zip(['dbname', 'user', 'password', 'port', 'host'], creds))
conn = psycopg2.connect(**pg_connection_dict)

aadt_for_db = gpd.read_file(os.path.join(datapath, 'MNDOT_AADT.geojson'))
sensors_df = gpd.read_file(os.path.join(datapath, 'PurpleAir_Stations.geojson'))


# clean up, dissolve by route_label / street volume, and select the high volume streets 
grouped_df = aadt_for_db.dropna().dissolve(by='ROUTE_LABEL', aggfunc={'STREET_NAME':'unique',
                                                            'CURRENT_VOLUME':'mean'}).reset_index()
important_street_names = grouped_df[(grouped_df.CURRENT_VOLUME > 1000)].STREET_NAME.explode().unique()

# select the traffic data from each street identified above. Only looking at those with linestring geometries 
focus_df = aadt_for_db[(aadt_for_db.STREET_NAME.isin(important_street_names))&
                        (aadt_for_db.geometry.type != 'MultiLineString')].copy() 

# Get the line trend (EastWest or NorthSouth) of each
focus_df['linetrend'] = focus_df.geometry.apply(lambda x: get_linetrend(x))
focus_df['id'] = np.arange(focus_df.shape[0]) #Create an id column
focus_df['geom_backup'] = focus_df.geometry

## get intersections of all roads
cross = gpd.sjoin(focus_df, focus_df, how = 'left', lsuffix = "x", rsuffix="y")
cross = cross.loc[cross['id_x'] < cross['id_y']] #Remove self joins
cross['inter'] = cross.geom_backup_x.intersection(cross.geom_backup_y) #Intersect them
cross = cross.set_geometry('inter')[[column for column in cross.columns if 'geom' not in column]] # set new geometry, drop old onees
cross['NS_cross_street'] = cross.apply(lambda row: row.STREET_NAME_x if row.linetrend_x == 'NS' else (row.STREET_NAME_y if row.linetrend_y == 'NS' else None), axis=1)
cross['EW_cross_street'] = cross.apply(lambda row: row.STREET_NAME_x if row.linetrend_x == 'EW' else (row.STREET_NAME_y if row.linetrend_y == 'EW' else None), axis=1)

intersections_df = cross[['NS_cross_street', 'EW_cross_street', 'inter']].dropna().copy().reset_index(drop=True)
is_duplicated = intersections_df[['NS_cross_street', 'EW_cross_street']].duplicated(keep = 'first')
is_samestreet = intersections_df.NS_cross_street == intersections_df.EW_cross_street
intersections_df_no_duplicates = intersections_df[(~is_duplicated)& (~is_samestreet)]

nearby_df = intersections_df_no_duplicates.copy()
nearby_df['nearby_sensors'] = nearby_df.apply(lambda row: Get_nearby_sensors(row.inter, sensors_df, 2000), axis=1)
nearby_df.astype({'nearby_sensors': 'str'}).to_file(os.path.join(datapath, 'Road_Intersections.geojson'))

df_for_db = nearby_df.copy()
sorted_df = df_for_db.drop('inter', axis=1).copy()
sorted_df['wkt'] = df_for_db.inter.apply(lambda x: x.wkt)
cols_for_db = list(df_for_db.columns[:-2]) + ['nearby_sensors', 'geometry']

cur = conn.cursor()
for index, row in sorted_df.iterrows():
    # This is really a great way to insert a lot of data
    q1 = sql.SQL('INSERT INTO "Road Intersections" ({}) VALUES ({},{});').format(
     sql.SQL(', ').join(map(sql.Identifier, cols_for_db)),
     sql.SQL(', ').join(sql.Placeholder() * (len(cols_for_db)-1)),
     # sql.SQL('%s'),
     sql.SQL('ST_Transform(ST_SetSRID(ST_GeomFromText(%s), 26915),4326)::geometry'))
    #print(q1.as_string(conn), (list(row.values)))
#     break

    cur.execute(q1.as_string(conn),
        (list(row.values))
        )
    conn.commit()    # Commit command

cur.close()# Close cursor
conn.close()# Close connection