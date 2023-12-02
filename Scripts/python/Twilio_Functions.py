'''
using this to handle all text sending functions. 
Drivers will be communicate_alert_start and communicate_alert_end
'''

import os
from twilio.rest import Client

# Getting .env information
from dotenv import load_dotenv
import time # Sleeping
import pytz # Timezones
import numpy as np

def send_texts(numbers, messages): # could refactor to send to user, that way we could inc. messages_sent in this function (better than havign to do it in parents)
    '''basic send function that takes in a list of numbers + list of messages and sends them out
    and returns a list of times that each message was sent
    '''

    load_dotenv()

    account_sid = os.environ['TWILIO_ACCOUNT_SID']
    auth_token = os.environ['TWILIO_AUTH_TOKEN']
    twilio_number = os.environ['TWILIO_NUMBER']
    messaging_service_sid = os.environ['TWILIO_SERVICE_SID']
    
    client = Client(account_sid, auth_token)

    times = []
    for number, message in zip(numbers, messages):

        msg = client.messages.create(
        body= message,
        messaging_service_sid=messaging_service_sid,
        from_=twilio_number,
        to=number # replace with number in PROD
        ) # should check error handling, if needed based on SDK
        
        time.sleep(1) # Sleeping for 1 second between sending messages
        
        times.append(msg.date_updated.replace(tzinfo=pytz.timezone('America/Chicago'))) # Append times sent - must change timezone in TWILIO otherwise I think it defaults to pacific time
        
    return times
    
def check_unsubscriptions(numbers):
    '''Returns the indices of the phone numbers in numbers that have unsubscribed
    Which corresponds to record_ids_to_text'''

    load_dotenv()

    account_sid = os.environ['TWILIO_ACCOUNT_SID']
    auth_token = os.environ['TWILIO_AUTH_TOKEN']
    
    unsubscribed_indices = []
    
    stop_key_words = ['STOP', 'STOPALL', 'UNSUBSCRIBE', 'CANCEL', 'END', 'QUIT'] # see https://support.twilio.com/hc/en-us/articles/223134027-Twilio-support-for-opt-out-keywords-SMS-STOP-filtering-

    # Set up Twilio Client

    client = Client(account_sid, auth_token)

    # Iterate through the numbers 
    
    for i, number in enumerate(numbers):
        
        messages_from = client.messages.list(from_=number) # Check if the numbers have responded, messages_from is a list twilio objects

        if len(messages_from) > 0: # If yes

            for message in messages_from: # What have they said?
    
                if message.body in stop_key_words: # If stopword

                    unsubscribed_indices += [i] # Keep track of that list index
                    
                    break

    return unsubscribed_indices
    
def delete_twilio_info(numbers):
    '''This function deletes texts to/from phone numbers in twilio
    '''
    
    load_dotenv()

    account_sid = os.environ['TWILIO_ACCOUNT_SID']
    auth_token = os.environ['TWILIO_AUTH_TOKEN']
    # Set up Twilio Client

    client = Client(account_sid, auth_token)

    # Iterate through the unique numbers 
    
    numbers_unique = np.unique(numbers)
    
    for number in numbers_unique:
        
        messages_from = client.messages.list(from_=number) # Check if the numbers have responded, messages_from is a list twilio objects
        
        for message in messages_from:
        
            message.delete()

        messages_to = client.messages.list(to_=number) # Get all messages we have sent to this number, messages_to is a list twilio objects
        
        for message in messages_to:
        
            message.delete()
