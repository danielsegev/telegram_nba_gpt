from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os
import logging
from airflow.utils.dates import days_ago

# Ensure Airflow can find the scripts directory
dag_folder = os.path.dirname(os.path.abspath(__file__))
scripts_path = os.path.join(dag_folder, "scripts")
include_path = os.path.join(dag_folder, "include", "scripts")

# Import database initialization scripts
from create_tables import create_database, create_tables

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email": ['your_email@example.com'],  # Replace with your email
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),  # FIX: Avoid future start date issues
}

# Initialize DAG
with DAG(
    "nba_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",  # FIX: Adjust schedule if needed
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

    # STEP 3: DATA PROCESSING WITH SPARK (RUNS IN PARALLEL)
    def spark_submit_task(task_id, script_name):
        return SparkSubmitOperator(
            task_id=task_id,
            conn_id="spark_default",
            application=f"/usr/local/airflow/include/scripts/{script_name}",  # FIXED PATH
            packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0",
            executor_memory="2G",
            driver_memory="1G",
            name=task_id.replace("_", " ").title(),
        )

    spark_csv_to_postgres_teams = spark_submit_task(
        "spark_csv_to_postgres_teams", "csv_to_postgres_teams.py"
    )

    spark_csv_to_postgres_players = spark_submit_task(
        "spark_csv_to_postgres_players", "csv_to_postgres_players.py"
    )

    spark_csv_to_postgres_games = spark_submit_task(
        "spark_csv_to_postgres_games", "csv_to_postgres_games.py"
    )

    create_tables_task >> [spark_csv_to_postgres_teams, spark_csv_to_postgres_players, spark_csv_to_postgres_games]
