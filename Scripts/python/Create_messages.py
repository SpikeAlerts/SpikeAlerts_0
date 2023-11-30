### Import Modules
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def new_alert_message(sensor_index, verified_number = True):
    '''
    Get a message for a new alert at a sensor_index
    # Composes and returns a single message
    '''
    
        
    # Short version (1 segment)
    
    message = f'''SPIKE ALERT!
Air quality is unhealthy in your area'''
    
    # URLs cannot be sent until phone number is verified
    if verified_number:
        message = message + f'''
https://map.purpleair.com/?select={int(sensor_index)}/44.9723/-93.2447'''
    else:
        message = message + '''
        Please see PurpleAir'''
        
    message = message + '''
    
Text STOP to unsubscribe'''
        
    return message


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def end_alert_message(duration, max_reading, report_id, base_report_url, verified_number = True):
    '''
    Get a list of messages to send when an alert is over

    inputs:
    duration = integer (number of minutes)
    max_reading = float
    report_id = string
    base_report_url is a string links directly to REDCap comment survey
    
    Returns a message (string)
    '''
        
    message = f'''Alert Over
Duration: {int(duration)} minutes 
Max value: {max_reading} ug/m3

Report here - '''
    
    # URLs cannot be sent until phone number is verified
    if verified_number:
        message = message + f"{base_report_url+ '&report_id=' + report_id}"
    else:
        message = message + f'URL coming soon... Report ID: {report_id}'
    # See https://help.redcap.ualberta.ca/help-and-faq/survey-parameters for filling in variable in url
        
    return message
    
def welcome_message():
    '''
    Composes a message welcoming a new user!
    '''
    
    message = '''Welcome to SpikeAlerts! 

We will text 8am-9pm when air quality seems unhealthy within 1 kilometer of your designated location.

If you have questions, please email mplsairquality@gmail.com or see https://SpikeAlerts.github.io/Website

Reply STOP to end this service'''

    return message
