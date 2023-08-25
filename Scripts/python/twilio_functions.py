import os
from twilio.rest import Client

account_sid = os.environ['TWILIO_ACCOUNT_SID']
auth_token = os.environ['TWILIO_AUTH_TOKEN']


# basic send function that takes in a list of numbers and a list of messages. 

def send_texts(numbers, messages):

    client = Client(account_sid, auth_token)

    i = 0
    for number in numbers:
        client.messages.create(
        body= messages[i],
        from_='+18777484881',
        to=number
        )
        i += 1




