# Basic PSQL Functions

## Load modules

import psycopg2

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

def insert_into(df, tablename, pg_connection_dict):
    '''
    Takes a well formatted dataframe, df,  
        with the columns aligned to the fields of a table in the database (tablename)
    as well as pg_connection_dict
    Inserts all rows into database
    And closes connection
    '''
    
    fieldnames = list(df.columns)
    
    # Create connection with postgres option keepalives_idle = 100 seconds
    conn = psycopg2.connect(**pg_connection_dict, 
                            keepalives_idle=100)
    
    # Create cursor
    cur = conn.cursor()
    
    for index, row in df.itertuples():
        
        vals = row[1:]
        
        q1 = sql.SQL(f'INSERT INTO {tablename}' + ' ({}) VALUES ({});').format(
     sql.SQL(', ').join(map(sql.Identifier, fieldnames)),
     sql.SQL(', ').join(sql.Placeholder() * (len(fieldnames))))

        cur.execute(q1.as_string(conn), (vals))
    
        conn.commit() # Committ command
    
    # Close cursor
    cur.close()
    # Close connection
    conn.close()
