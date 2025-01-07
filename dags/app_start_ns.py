from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os
import logging
import sys

sys.path.append("/opt/airflow/include/scripts")

from create_tables import create_database, create_tables

# Ensure Airflow can find the scripts directory
dag_folder = os.path.dirname(os.path.abspath(__file__))
scripts_path = os.path.join(dag_folder, "scripts")
include_path = os.path.join(dag_folder, "include", "scripts")

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

    # STEP 2: CSV DATA INGESTION
    def ingest_teams():
        import subprocess
        subprocess.run(["python", "/opt/airflow/include/scripts/csv_to_postgres_teams.py"], check=True)

    def ingest_players():
        import subprocess
        subprocess.run(["python", "/opt/airflow/include/scripts/csv_to_postgres_players.py"], check=True)

    def ingest_games():
        import subprocess
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

    # STEP 3: INGEST HISTORICAL GAMES (FROM API TO KAFKA)
    def ingest_season_games():
        import subprocess
        subprocess.run(["python", "/opt/airflow/include/scripts/fetch_games_to_kafka.py"], check=True)

    ingest_season_games_task = PythonOperator(
        task_id="ingest_season_games",
        python_callable=ingest_season_games,
    )

    # STEP 4: SPARK JOB - LOAD KAFKA GAMES DATA INTO POSTGRES
    spark_ingest_games_task = SparkSubmitOperator(
        task_id="spark_ingest_games",
        conn_id="spark_default",
        application="/opt/airflow/include/scripts/kafka_to_postgres_games.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0",
        executor_memory="2G",
        driver_memory="1G",
        name="Ingest Historical Games Data from Kafka to Postgres",
        conf={
            "spark.executor.extraJavaOptions": "-Dlog4j.configuration=log4j.properties",
            "spark.driverEnv.SPARK_HOME": "/opt/spark",
            "spark.executorEnv.SPARK_HOME": "/opt/spark",
            "spark.driverEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
            "spark.executorEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
        },
    )

    # Task dependencies
    create_tables_task >> [ingest_teams_task, ingest_players_task, ingest_games_task]
    ingest_games_task >> ingest_season_games_task >> spark_ingest_games_task
