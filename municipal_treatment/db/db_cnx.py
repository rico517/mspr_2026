import time
import mysql.connector
from mysql.connector import Error

# Maximum number of connection attempts
MAX_RETRIES = 5
# Delay between retries in seconds
RETRY_DELAY = 10

DB_HOST ='localhost'
DB_PORT = 3306
DB_USER = 'root'
DB_PASSWORD = 'root'
DATABASE= 'elections_db'

def connect_to_database():
    """
    Establishes a connection to the database with a retry mechanism.

    Args:
        host: Host of the database
        port: Port of the database
        user: Username
        password: Password
        database: Name of the database
        retries: Number of retry attempts

    Returns:
        Connection to the database

    Raises:
        Exception: If connection fails after all retry attempts
    """
    for attempt in range(MAX_RETRIES):
        try:
            # print(f"Trying to connect to {DATABASE} ({attempt + 1}/{MAX_RETRIES})...")
            conn = mysql.connector.connect(
                host=DB_HOST, port=DB_PORT,
                user=DB_USER, password=DB_PASSWORD, database=DATABASE,
                connect_timeout=30,auth_plugin='mysql_native_password'
            )
            # print(f"Connection to {DATABASE} established successfully")
            return conn
        except Error as e:
            print(f"Error connecting to {DATABASE}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                print(f"New attempt in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"Failed to connect to {DATABASE} after {MAX_RETRIES} attempts")
                raise