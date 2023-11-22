import os # For working with Operating System
from urllib.parse import quote
from dotenv import load_dotenv # Loading .env info
load_dotenv()

if os.getenv('DATABASE_URL'):
  url = os.getenv('DATABASE_URL')+ '?options=' + quote(os.getenv('DB_OPTIONS'))
  print(url)
  pg_connection_dict = url
else:
  creds = [os.getenv('DB_NAME'),
          os.getenv('DB_USER'),
          os.getenv('DB_PASS'),
          os.getenv('DB_PORT'),
          os.getenv('DB_HOST'),
          os.getenv('DB_OPTIONS')
          ]
  print(creds)
  pg_connection_dict = dict(zip(['dbname', 'user', 'password', 'port', 'host', 'options'], creds))
