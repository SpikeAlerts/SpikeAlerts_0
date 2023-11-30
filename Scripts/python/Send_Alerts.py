### Import Packages

# File Manipulation

import os # For working with Operating System
#import sys # System arguments
from dotenv import load_dotenv # Loading .env info

# Database 

import Basic_PSQL as psql
import psycopg2
from psycopg2 import sql

# Data Manipulation

import numpy as np
import pandas as pd

# Our functions

import Twilio_Functions as our_twilio 
import REDCap_Functions as redcap

load_dotenv()

creds = [os.getenv('DB_NAME'),
         os.getenv('DB_USER'),
         os.getenv('DB_PASS'),
         os.getenv('DB_PORT'),
         os.getenv('DB_HOST')
        ]

pg_connection_dict = dict(zip(['dbname', 'user', 'password', 'port', 'host'], creds)) 
  
# ~~~~~~~~~~~~~~ 
   
def send_all_messages(record_ids, messages, redCap_token_signUp, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_NUMBER, pg_connection_dict):
    '''
    This function will
    1. Send each message to the corresponding record_id
    2. update the user signup data to reflect each new message sent (+1 messages_sent, time added)

    Assumptions: 
    - We won't message the same user twice within an invocation of this function. Otherwise we might need to aggregate the data before step #2
    '''
    
    #import twilio_functions # This didn't work with my version yet, leaving for future reference
    
    numbers = redcap.Get_phone_numbers(record_ids, redCap_token_signUp) # See Send_Alerts.py
    
    # Check Unsubscriptions
    
    unsubscribed_indices = our_twilio.check_unsubscriptions(numbers, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) # See twilio_functions.py
    
    if len(unsubscribed_indices) > 0:
    
        # Unsubscribe from our database - see Send_Alerts.py
        record_ids_to_unsubscribe = list(np.array(record_ids)[unsubscribed_indices])
        Unsubscribe_users(record_ids_to_unsubscribe, pg_connection_dict)
        # Delete Twilio Information - see twilio_functions.py
        numbers_to_unsubscribe = list(np.array(numbers)[unsubscribed_indices])
        our_twilio.delete_twilio_info(numbers_to_unsubscribe, account_sid, auth_token)
        
        # pop() unsubscriptions from numbers/record_ids/messages list
        
        for unsubscribed_index in unsubscribed_indices:
            
            numbers.pop(unsubscribed_index)
            record_ids.pop(unsubscribed_index)
            messages.pop(unsubscribed_index)
        
    # Send messages
    
    times = our_twilio.send_texts(numbers, messages, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_NUMBER) # See twilio_functions.py
    
    update_user_table(record_ids, times, pg_connection_dict) # See Send_Alerts.py

    return

# ~~~~~~~~~~~~~

def update_user_table(record_ids, times, pg_connection_dict):
    '''
    Takes a list of users + time objects and updates the "Sign Up Information" table
    to increment each user's messages_sent and last_messaged
    '''
    #print("updating Sign Up Information", record_ids, times)

    conn = psycopg2.connect(**pg_connection_dict,keepalives_idle=10)
    cur = conn.cursor()

    # dataframe is sorted by record ID because SQL messages_sent query needs to be ordered (and this needs to match
    sorted = pd.DataFrame({'record_ids': record_ids,'times': times}).sort_values(by = "record_ids")  

    cmd = sql.SQL('''
    SELECT messages_sent
    FROM "Sign Up Information" u
    WHERE u.record_id = ANY ( {} )
    ORDER BY u.record_id asc; 
    ''').format(sql.Literal(record_ids))

    cur.execute(cmd)
    conn.commit()

    messages_sent_list = [i[0] for i in cur.fetchall()] # Unpack results into list
    messages_sent_new = [v+1 for v in messages_sent_list]
    sorted["messages_sent_new"] = messages_sent_new

    # SQL statement that updates each record (identified by record_ids) with new times, messages_sent_new values
    # if this ever has performance trouble, we could try https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html
    # which would require record_id to be made into a foreign key  
    for id, time, msg_inc in zip(sorted["record_ids"], sorted["times"], sorted["messages_sent_new"]):
        cmd = sql.SQL('''
        UPDATE "Sign Up Information"
        SET last_messaged = {lm}, messages_sent = {ms} 
        WHERE record_id =  {ri} ;
        ''').format(ri = sql.Literal(id),
                    lm = sql.Literal(time),
                    ms = sql.Literal(msg_inc)
                    )
        cur.execute(cmd)
    
    conn.commit()
    
    cur.close()
    conn.close()
    return
    
# ~~~~~~~~~~~~~
    
def Unsubscribe_users(record_ids, pg_connection_dict):
    '''
    Change record_ids to subscribed = FALSE in our database <- checked before every message, not daily... 
    '''
    
    cmd = sql.SQL('''UPDATE "Sign Up Information"
    SET subscribed = FALSE
WHERE record_id = ANY ( {} );
    ''').format(sql.Literal(record_ids))
    
    psql.send_update(cmd, pg_connection_dict)
