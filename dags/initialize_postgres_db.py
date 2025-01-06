from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import sys
import os

# ✅ Add this to make the include directory discoverable
sys.path.append(os.path.join(os.path.dirname(__file__), "include"))

# ✅ Now, import the module
from scripts.create_tables import create_database, create_tables

# Define logging
logger = logging.getLogger("airflow.task")

# Updated create_database function with logging
def create_database_with_logging():
    try:
        logger.info("Starting database creation...")
        create_database()
        logger.info("Database creation completed successfully.")
    except Exception as e:
        logger.error("Database creation failed:", exc_info=True)
        raise

# Updated create_tables function with logging
def create_tables_with_logging():
    try:
        logger.info("Starting table creation...")
        create_tables()
        logger.info("Table creation completed successfully.")
    except Exception as e:
        logger.error("Table creation failed:", exc_info=True)
        raise

# Define the DAG
with DAG(
    "initialize_postgres_db",
    default_args={
        "owner": "postgres",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Initialize PostgreSQL Database and Tables",
    schedule=None,  # Use `schedule` instead of `schedule_interval`
    start_date=datetime(2023, 12, 27),
    catchup=False,
) as dag:

    # Task to create the database
    create_db_task = PythonOperator(
        task_id="create_database",
        python_callable=create_database_with_logging,  # Use the updated function
    )

    # Task to create the tables
    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables_with_logging,  # Use the updated function
    )

    # Task dependencies
    create_db_task >> create_tables_task
