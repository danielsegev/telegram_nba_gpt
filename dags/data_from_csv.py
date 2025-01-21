from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os
import logging
import sys
import subprocess

sys.path.append("/opt/airflow/include/scripts")

from include.scripts.create_tables import create_database, create_tables

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email": ['your_email@example.com'],
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2023, 12, 29),  # Fixed date, only runs manually
}

# Initialize DAG (MANUAL TRIGGER ONLY)
with DAG(
    "nba_historical_data_pipeline",
    default_args=default_args,
    schedule_interval=None,  # Only runs manually
    catchup=False,
) as dag:

    # STEP 1: DATABASE INITIALIZATION
    def create_database_with_logging():
        logging.info("Starting database creation...")
        create_database()
        logging.info("Database creation completed.")

    def create_tables_with_logging():
        logging.info("Starting table creation...")
        create_tables()
        logging.info("Table creation completed.")

    initialize_postgres_db = PythonOperator(
        task_id="initialize_postgres_db",
        python_callable=create_database_with_logging,
    )

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables_with_logging,
    )

    initialize_postgres_db >> create_tables_task

    # STEP 2: CSV DATA INGESTION (REVERTED TO WORKING VERSION)
    def ingest_teams():
        subprocess.run(["python", "/opt/airflow/include/scripts/csv_to_postgres_teams.py"], check=True)

    def ingest_players():
        subprocess.run(["python", "/opt/airflow/include/scripts/csv_to_postgres_players.py"], check=True)

    def ingest_games():
        subprocess.run(["python", "/opt/airflow/include/scripts/csv_to_postgres_games.py"], check=True)

    ingest_teams_task = PythonOperator(
        task_id="ingest_teams",
        python_callable=ingest_teams,
    )

    ingest_players_task = PythonOperator(
        task_id="ingest_players",
        python_callable=ingest_players,
    )

    ingest_games_task = PythonOperator(
        task_id="ingest_games",
        python_callable=ingest_games,
    )

    # Task dependencies
    create_tables_task >> [ingest_teams_task, ingest_players_task, ingest_games_task]
