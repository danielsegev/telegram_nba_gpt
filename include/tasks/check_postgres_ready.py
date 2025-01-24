import psycopg2
import os
import time
import logging

def check_postgres_ready():
    """
    Attempts to connect to PostgreSQL to ensure it is running.
    Retries up to 5 times before failing.
    """
    db_config = {
        "dbname": os.getenv("POSTGRES_DB", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
    }

    for attempt in range(1, 6):
        try:
            logging.info("Checking if PostgreSQL is ready (attempt %d)...", attempt)
            conn = psycopg2.connect(**db_config)
            conn.close()
            logging.info("PostgreSQL is ready!")
            return True
        except psycopg2.OperationalError as e:
            logging.warning("PostgreSQL is not ready yet. Retrying in 5 seconds...")
            time.sleep(5)

    logging.error("PostgreSQL did not become ready after multiple attempts.")
    raise RuntimeError("PostgreSQL is not available.")

if __name__ == "__main__":
    check_postgres_ready()
