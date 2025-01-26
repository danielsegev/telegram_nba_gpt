import psycopg2
import os
import logging
import sys

def truncate_postgres_table(table_name):
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("truncate_postgres_table")

    # Fetch environment variables
    dbname = os.getenv("dwh")
    user = os.getenv("airflow")
    password = os.getenv("airflow")
    host = os.getenv("postgres")
    port = os.getenv("5432")

    if not all([dbname, user, password, host, port]):
        logger.error("❌ One or more environment variables are not set. Check 'dwh', 'airflow', 'postgres', and '5432'.")
        return

    if not table_name:
        logger.error("❌ Table name must be provided as a parameter.")
        return

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cur = conn.cursor()
        logger.info("✅ Successfully connected to PostgreSQL.")

        # Truncate the specified table
        truncate_query = f"TRUNCATE TABLE public.{table_name} RESTART IDENTITY CASCADE;"
        cur.execute(truncate_query)
        logger.info(f"✅ Executed: {truncate_query}")

        conn.commit()
        logger.info(f"✅ Table '{table_name}' truncated successfully.")

    except psycopg2.Error as e:
        logger.error(f"❌ PostgreSQL error: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    finally:
        # Ensure resources are closed properly
        if 'cur' in locals():
            cur.close()
            logger.info("🔒 Cursor closed.")
        if 'conn' in locals():
            conn.close()
            logger.info("🔒 Connection closed.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python truncate_postgres_table.py <table_name>")
    else:
        table_name = sys.argv[1]
        truncate_postgres_table(table_name)
