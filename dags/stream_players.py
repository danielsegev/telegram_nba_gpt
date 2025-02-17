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
    from tasks.kafka_producer_players import fetch_and_send_players
    from tasks.truncate_postgres_tables import truncate_postgres_table  # Import the function
except ImportError as e:
    raise ImportError(f"Error importing module: {e}")

# Initialize DAG
with DAG(
    "nba_stream_players",
    default_args=default_args,
    schedule_interval="0 0 1 9 *",  # Runs annually on September 1st at midnight
    start_date=datetime(2023, 9, 1),
    catchup=False,
) as dag:

    # Task: Truncate `dim_player` table
    truncate_dim_player_task = PythonOperator(
        task_id="truncate_dim_player",
        python_callable=truncate_postgres_table,
        op_args=["dim_player"],  # Pass 'dim_player' as the table name
    )

    # Task: Produce player data to Kafka
    kafka_producer_players_task = PythonOperator(
        task_id="kafka_producer_players",
        python_callable=fetch_and_send_players,
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

    # Task: Process player data from Kafka to PostgreSQL
    spark_process_players_task = spark_submit_task(
        "spark_process_players", "kafka_to_postgres_players.py"
    )

    # Define task dependencies
    truncate_dim_player_task >> kafka_producer_players_task >> spark_process_players_task
