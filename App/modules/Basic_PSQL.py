# Basic PSQL Functions

## Load modules

import psycopg2
from psycopg2 import sql

# ~~~~~~~~~~~~~~

def send_update(cmd, pg_connection_dict):
    '''
    Takes a command (sql.SQL() string) and pg_connection_dict
    Sends the command to postgres
    And closes connection
    '''
    
    # Create connection with postgres option keepalives_idle = 1 seconds
    conn = psycopg2.connect(**pg_connection_dict, 
                            keepalives_idle=1)
    
    # Create cursor
    cur = conn.cursor()
    
    cur.execute(cmd) # Execute
    
    conn.commit() # Committ command
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
    
# ~~~~~~~~~~~~~~

def get_response(cmd, pg_connection_dict):
    '''
    Takes a command (sql.SQL() string) and pg_connection_dict
    Sends the command to postgres
    Retrieves the response
    And closes connection
    '''
    
    # Create connection with postgres option keepalives_idle = 30 seconds
    conn = psycopg2.connect(**pg_connection_dict, 
                            keepalives_idle=30)
    
    # Create cursor
    cur = conn.cursor()
    
    cur.execute(cmd) # Execute
    
    conn.commit() # Committ command
    
    # Fetch Response
    
    response = cur.fetchall()
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
    
    return response
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~

def insert_into(df, tablename, pg_connection_dict, is_spatial = False):
    '''
    Takes a well formatted dataframe, df,  
        with the columns aligned to the fields of a table in the database (tablename)
    as well as pg_connection_dict
    Inserts all rows into database
    And closes connection
    
    IF YOU ARE INSERTING A SPATIAL DATASET - please indicate this by setting the is_spatial variable = True
    And be sure that the last column is called geometry with well known text in WGS84 (EPSG:4326) "Lat/lon"
    '''
    
    
    fieldnames = list(df.columns)
    
    # Create connection with postgres option keepalives_idle = 100 seconds
    conn = psycopg2.connect(**pg_connection_dict, 
                            keepalives_idle=100)
    
    # Create cursor
    cur = conn.cursor()
    
    for row in df.itertuples():
        
        vals = row[1:]
        
        if is_spatial: # We need to treat the geometry column of WKT a little differently
        
            q1 = sql.SQL(f'INSERT INTO "{tablename}"' + ' ({}) VALUES ({},{});').format(
     sql.SQL(', ').join(map(sql.Identifier, fieldnames)),
     sql.SQL(', ').join(sql.Placeholder() * (len(fieldnames) - 1)),
     sql.SQL('ST_SetSRID(ST_GeomFromText(%s), 4326)::geometry'))
        
        else:
        
            q1 = sql.SQL(f'INSERT INTO "{tablename}"' + ' ({}) VALUES ({});').format(
     sql.SQL(', ').join(map(sql.Identifier, fieldnames)),
     sql.SQL(', ').join(sql.Placeholder() * (len(fieldnames))))

        # Execute command
        cur.execute(q1.as_string(conn), (vals))
    
        conn.commit() # Commit command
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
