# database.py
import psycopg2
from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def query_postgresql(query_text):
    try:
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
