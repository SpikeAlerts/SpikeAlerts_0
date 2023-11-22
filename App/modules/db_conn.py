import os # For working with Operating System
from urllib.parse import urlparse, quote
from dotenv import load_dotenv # Loading .env info
load_dotenv()

if os.getenv('DATABASE_URL'):
  parsed_url = urlparse(os.getenv('DATABASE_URL'))
  print(parsed_url)

  creds = [parsed_url.path[1:],
        parsed_url.username,
        parsed_url.password,
        parsed_url.hostname,
        parsed_url.port,
        quote(os.getenv('DB_OPTIONS'))
        ]
  print(creds)
  pg_connection_dict = dict(zip(['dbname', 'user', 'password', 'port', 'host', 'options'], creds))
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
