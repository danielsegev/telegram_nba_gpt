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
    from tasks.create_tables import create_database, create_tables
    from tasks.truncate_postgres_tables import truncate_postgres_tables
    from tasks.kafka_producer_games import fetch_and_process_games
    from tasks.kafka_producer_players import fetch_and_send_players
    from tasks.kafka_producer_teams import send_teams_to_kafka
    from tasks.truncate_postgres_tables import truncate_postgres_tables
except ImportError as e:
    raise ImportError(f"Error importing module: {e}")

# Initialize DAG
with DAG(
    "nba_full_data_pipeline_from_tasks",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task 1: Truncate PostgreSQL tables
    truncate_tables_task = PythonOperator(
        task_id="truncate_tables",
        python_callable=truncate_postgres_tables,
    )

    # Task 2: Create PostgreSQL database
    create_postgres_db_task = PythonOperator(
        task_id="create_postgres_db",
        python_callable=create_database,
    )

    create_postgres_tables_task = PythonOperator(
        task_id="create_postgres_tables",
        python_callable=create_tables,
    )

    # Task 3: Produce game data to Kafka
    kafka_producer_games_task = PythonOperator(
        task_id="kafka_producer_games",
        python_callable=fetch_and_process_games,
        op_args=["2024-25"],
    )

    # Task 4: Produce player data to Kafka
    kafka_producer_players_task = PythonOperator(
        task_id="kafka_producer_players",
        python_callable=fetch_and_send_players,
    )

    # Task 5: Produce team data to Kafka
    kafka_producer_teams_task = PythonOperator(
        task_id="kafka_producer_teams",
        python_callable=send_teams_to_kafka,
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

    # Task 7: Process player data from Kafka to PostgreSQL
    spark_process_players_task = spark_submit_task(
        "spark_process_players", "kafka_to_postgres_players.py"
    )

    # Task 8: Process team data from Kafka to PostgreSQL
    spark_process_teams_task = spark_submit_task(
        "spark_process_teams", "kafka_to_postgres_teams.py"
    )

    # Define task dependencies
    # truncate_tables_task >> create_postgres_db_task
    create_postgres_db_task >> create_postgres_tables_task 
    create_postgres_tables_task >> [
        kafka_producer_games_task,
        kafka_producer_players_task,
        kafka_producer_teams_task,
    ]
    kafka_producer_games_task >> spark_process_games_task
    kafka_producer_players_task >> spark_process_players_task
    kafka_producer_teams_task >> spark_process_teams_task
