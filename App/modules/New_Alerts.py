### Import Packages

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 

from App.modules import Basic_PSQL as psql
from App.modules import Our_Queries as query
import psycopg2
from psycopg2 import sql

# Data Manipulation

import pandas as pd

# Messaging

from App.modules import Create_messages

## Workflow

def workflow(new_spikes_df, purpleAir_runtime, messages, record_ids_to_text, can_text, pg_connection_dict):
    '''
    Runs the full workflow for a new spike
    
    Needs new_spikes_df (pd.DataFrame with columns 'sensor_index' int  and 'pm25' float, 
            purpleAir_runtime (datetime timestamp))
    '''
    
    # NEW Spikes - for every newly alerted sensor we should...
    
    for _, row in new_spikes_df.iterrows():

        # 1) Add to active alerts
    
        newest_alert_index = add_to_active_alerts(row,
                                                 pg_connection_dict,
                                                 purpleAir_runtime # When we ran the PurpleAir Query
                                                 )
        
        # 2) Query users ST_Dwithin 1000 meters & subscribed = TRUE
        
        record_ids_nearby = query.Get_active_users_nearby_sensor(pg_connection_dict, row.sensor_index, 1000) # in Send_Alerts.py
        
        if len(record_ids_nearby) > 0:

            if can_text: # Waking Hours
        
                # a) Query users from record_ids_nearby if both active_alerts and cached_alerts are empty
                record_ids_new_alerts = query.Get_users_to_message_new_alert(pg_connection_dict, record_ids_nearby) # in Send_Alerts.py & .ipynb 
                
                # Compose Messages & concat to messages/record_id_to_text   
                
                # Add to message/record_id storage for future messaging
                record_ids_to_text += record_ids_new_alerts
                messages += [Create_messages.new_alert_message(row.sensor_index)]*len(record_ids_new_alerts) # in Create_Messages.py
                
            # b) Add newest_alert_index to record_ids_nearby's Active Alerts
            Update_users_active_alerts(record_ids_nearby, newest_alert_index, pg_connection_dict) 
            
    return messages, record_ids_to_text
            
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    
def add_to_active_alerts(row, pg_connection_dict, purpleAir_runtime):
    '''
    This takes a row from spikes_df,
    the connection dictionary,
    runtime_for_db = datetime when purpleair was queried
    
    it returns the alert_index that it created
    
    '''
    cols_for_db = ['sensor_indices', 'start_time', 'max_reading']
    sensor_indices = [row.sensor_index] # We haven't started to cluster alerts yet
    runtime_for_db = purpleAir_runtime.strftime('%Y-%m-%d %H:%M:%S')
    reading = float(row.pm25)

    # Create Cursor for commands
    conn = psycopg2.connect(**pg_connection_dict)
    cur = conn.cursor()
    
    # This is really a great way to insert a lot of data

    vals = [sensor_indices, runtime_for_db, reading]
    
    q1 = sql.SQL('INSERT INTO "Active Alerts Acute PurpleAir" ({}) VALUES ({});').format(
     sql.SQL(', ').join(map(sql.Identifier, cols_for_db)),
     sql.SQL(', ').join(sql.Placeholder() * (len(cols_for_db))))

    cur.execute(q1.as_string(conn),
        (vals)
        )
    # Commit command
    conn.commit()
    
    # Get alert_index we just created
    
    cmd = sql.SQL('''SELECT alert_index
    FROM "Active Alerts Acute PurpleAir"
    WHERE sensor_indices = {}::int[];'''
             ).format(sql.Literal(sensor_indices))
    
    cur.execute(cmd)     
    
    conn.commit() # Committ command
    
    newest_alert_index = cur.fetchall()[0][0]
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
    return newest_alert_index
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Update User's Active Alerts

def Update_users_active_alerts(record_ids, alert_index, pg_connection_dict):
    '''
    This function takes a list of record_ids (users), an alert index (integer), and pg_connection_dict

    It will add this alert index to all the record_ids' active_alerts
    '''
    
    cmd = sql.SQL('''
UPDATE "Sign Up Information"
SET active_alerts = ARRAY_APPEND(active_alerts, {}) -- inserted alert_index
WHERE record_id = ANY ( {} ); -- inserted record_ids 
    ''').format(sql.Literal(alert_index),
                sql.Literal(record_ids)
               )

    psql.send_update(cmd, pg_connection_dict)
