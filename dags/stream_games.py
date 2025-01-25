from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os
import sys

# Get the directory of the DAG file
dag_folder = os.path.dirname(os.path.abspath(__file__))
tasks_path = os.path.join(dag_folder, "include", "tasks")

# Add the tasks directory to the system path
if tasks_path not in sys.path:
    sys.path.insert(0, tasks_path)

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 6),
}

# Import external scripts
try:
    from tasks.kafka_producer_games import fetch_and_process_games
except ImportError as e:
    raise ImportError(f"Error importing module: {e}")

# Initialize DAG
with DAG(
    "nba_stream_games",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # Runs daily at 3 AM
    start_date=datetime(2023, 12, 29),
    catchup=False,
) as dag:

    # Task 3: Produce game data to Kafka
    kafka_producer_games_task = PythonOperator(
        task_id="kafka_producer_games",
        python_callable=fetch_and_process_games,
        op_args=["2024-25"],
    )

    # Spark Task Templates
    def spark_submit_task(task_id, script_name):
        return SparkSubmitOperator(
            task_id=task_id,
            conn_id="spark_default",
            application=f"/opt/airflow/include/scripts/{script_name}",
            packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0",
            executor_memory="2G",
            driver_memory="1G",
            name=task_id.replace("_", " ").title(),
        )

    # Task 6: Process game data from Kafka to PostgreSQL
    spark_process_games_task = spark_submit_task(
        "spark_process_games", "kafka_to_postgres_games.py"
    )

    kafka_producer_games_task >> spark_process_games_task
