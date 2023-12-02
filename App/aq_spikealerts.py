## import

import os # For working with Operating System

from dotenv import load_dotenv # Loading .env info
from App.modules.db_init import db_init, db_need_init, db_notinit
from App.modules import MAIN
from App.modules import Twilio_Functions as our_twilio

## variables

load_dotenv() # load .env file

if db_need_init() == True:
  print("Minneapolis Boundaries table empty. running db init.")
  db_init()

try:
    MAIN.main_loop()
    
except Exception as e:
    
    print(e)
    
finally:

    our_twilio.send_texts([os.environ['LOCAL_PHONE']], ['SpikeAlerts Down'])   
