
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

ASTRA_DB_TOKEN = os.getenv("ASTRA_DB_TOKEN")
ASTRA_DB_API_ENDPOINT = os.getenv("ASTRA_DB_API_ENDPOINT")
KEYSPACE = os.getenv("KEYSPACE")