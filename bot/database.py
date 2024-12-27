import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Retrieve PostgreSQL connection details from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def query_postgresql(query_text):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(query_text)
        result = cursor.fetchall()

        # Close the connection
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        return f"Database query failed: {e}"
