### Import Packages

# Database 

from App.modules import Basic_PSQL as psql
from psycopg2 import sql

## Workflow

def workflow(ongoing_spikes_df, pg_connection_dict):
    '''
    Runs the full workflow for an ongoing spike
    
    Needs ongoing_spikes_df (pd.DataFrame with columns 'sensor_index' int  and 'pm25' float)
    '''
    
    # Ongoing Spikes - for every Ongoing alerted sensor we should..            


    for _, spike in ongoing_spikes_df.iterrows():

        # 1) Update the maximum reading

        Update_max_reading(spike, pg_connection_dict)
        
        # 2) Merge/Cluster alerts? 
        # NOT DONE - FAR FUTURE TO DO

# ~~~~~~~~~~~~~~~~~~~~~

def Update_max_reading(row, pg_connection_dict):
    '''
    Row should be a row from the ongoing_spikes dataFrame
    eg. spikes_df[spikes_df.sensor_index.isin(ongoing_spike_sensors)]
    '''

    sensor_index = row.sensor_index
    reading = row.pm25
    
    cmd = sql.SQL('''
UPDATE "Active Alerts Acute PurpleAir"
SET max_reading = GREATEST({}, max_reading)
WHERE {} = ANY (sensor_indices);

''').format(sql.Literal(reading), sql.Literal(sensor_index))

    psql.send_update(cmd, pg_connection_dict)
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
