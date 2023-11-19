## import

import os # For working with Operating System
import sys
from dotenv import load_dotenv # Loading .env info
from flask import Flask 
from modules.db_init import db_init, db_need_init, db_notinit
from modules.MAIN import main_loop


## variables

load_dotenv() # load .env file

app = Flask(__name__)
FLASK_HOST=os.getenv('FLASK_HOST')
FLASK_PORT=os.getenv('FLASK_PORT')

## server
print("name is", __name__, FLASK_PORT)

if db_need_init() == True:
  print("Minneapolis Boundaries table empty. running db init.")
  # db_init()
# main_loop()

# main_loop()

@app.route("/")
def hello_world():
  return "<!DOCTYPE html><body><p>Hello, dark, dark World!</p><a href='http://localhost:5000/init'><button>init db</button></a><body/></html>"

@app.route("/init")
def init_db():
  db_notinit()
  return "<p>initialized!</p>"

if __name__ == "__main__":
  app.run(host=FLASK_HOST, port=FLASK_PORT, debug=True)