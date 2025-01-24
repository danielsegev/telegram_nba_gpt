import psycopg2
import os
import logging

def truncate_postgres_tables():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("truncate_postgres_tables")

    # Fetch environment variables
    dbname = os.getenv("dwh")
    user = os.getenv("airflow")
    password = os.getenv("airflow")
    host = os.getenv("postgres")
    port = os.getenv("5432")

    if not all([dbname, user, password, host, port]):
        logger.error("❌ One or more environment variables are not set. Check 'dwh', 'airflow', 'postgres', and '5432'.")
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

        # Truncate tables
        truncate_queries = [
            "TRUNCATE TABLE public.dim_team RESTART IDENTITY CASCADE;",
            "TRUNCATE TABLE public.dim_player RESTART IDENTITY CASCADE;",
            "TRUNCATE TABLE public.fact_game RESTART IDENTITY CASCADE;"
        ]

        for query in truncate_queries:
            cur.execute(query)
            logger.info(f"✅ Executed: {query}")

        conn.commit()
        logger.info("✅ All tables truncated successfully.")

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
    truncate_postgres_tables()
