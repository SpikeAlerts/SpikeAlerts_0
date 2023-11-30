### Import Packages

# File Manipulation

from io import StringIO

# Web

import requests # Accessing the Web

# Data Manipulation

import pandas as pd
import geopandas as gpd

# ~~~~~~~~~~~~~

def Get_phone_numbers(record_ids, redCap_token_signUp):
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

def Get_new_users(max_record_id, redCap_token_signUp):
    '''
    This function gets the newest user's record_id and wkt
    
    returns a pandas dataframe with phone (string), record_id (string) and wkt (string)
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

        focus_df = gdf[['phone', 'record_id', 'wkt']].copy()
        
    else:
        focus_df = pd.DataFrame() # No data obtained
    
    return focus_df
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
