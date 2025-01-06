from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import sys
import os

# Configure DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email': ['your_email@example.com'],  # Replace with your email
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 6)
}

with DAG(
    dag_id='activate_telegram_bot',
    default_args=default_args,
    schedule_interval=None,
    catchup=False  # Important for on-demand DAGs
) as dag:

    # Sensors for the data ingestion DAGs
    wait_for_teams_dag = ExternalTaskSensor(
        task_id='wait_for_teams_dag',
        external_dag_id='spark_kafka_to_postgres_teams_dag',  # Replace with your teams DAG ID
        external_task_id=None,  # Wait for the entire DAG to complete
        timeout=3600,  # Timeout in seconds (1 hour) - adjust as needed
        allowed_states=['success'], # Only trigger when the external DAG is successful
        execution_timeout=timedelta(minutes=60)
    )

    wait_for_players_dag = ExternalTaskSensor(
        task_id='wait_for_players_dag',
        external_dag_id='spark_kafka_to_postgres_players_dag',  # Replace with your players DAG ID
        external_task_id=None,  # Wait for the entire DAG to complete
        timeout=3600,  # Timeout in seconds (1 hour) - adjust as needed
        allowed_states=['success'], # Only trigger when the external DAG is successful
        execution_timeout=timedelta(minutes=60)
    )

    wait_for_games_dag = ExternalTaskSensor(
        task_id='wait_for_games_dag',
        external_dag_id='spark_kafka_to_postgres_games_dag',  # Replace with your games DAG ID
        external_task_id=None,  # Wait for the entire DAG to complete
        timeout=3600,  # Timeout in seconds (1 hour) - adjust as needed
        allowed_states=['success'], # Only trigger when the external DAG is successful
        execution_timeout=timedelta(minutes=60)
    )

    def run_bot():
        bot_dir = os.path.join(os.path.dirname(__file__), 'bot')
        sys.path.insert(0, bot_dir)
        from bot import main
        main()

    run_telegram_bot = PythonOperator(
        task_id='run_telegram_bot',
        python_callable=run_bot,
    )

    # Set dependencies using a list for parallel sensing
    [wait_for_teams_dag, wait_for_players_dag, wait_for_games_dag] >> run_telegram_bot