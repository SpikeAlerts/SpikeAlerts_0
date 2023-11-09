'''
using this to handle all text sending functions. 
Drivers will be communicate_alert_start and communicate_alert_end
'''

import os
from twilio.rest import Client
# Getting .env information
from dotenv import load_dotenv

load_dotenv()

account_sid = os.environ['TWILIO_ACCOUNT_SID']
auth_token = os.environ['TWILIO_AUTH_TOKEN']

def send_texts(numbers, messages): 
    '''basic send function that takes in a list of numbers + list of messages and sends them out
    and returns a list of times that each message was sent
    '''
    client = Client(account_sid, auth_token)

    times = []
    for number, message in zip(numbers, messages):
        print(number, message)
        msg = client.messages.create(
        body= message,
        from_=os.environ['TWILIO_NUMBER'],
        to=os.environ['LOCAL_PHONE'] # replace with number in PROD
        ) # should check error handling, if needed based on SDK
        times.append(msg.date_updated)

    return times
