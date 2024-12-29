from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from helpers.kafka_helpers import initialize_producer, produce_message
from nba_api.stats.static import teams
import pandas as pd
import json

# Function to send NBA teams to Kafka
def send_teams_to_kafka():
    producer = initialize_producer()  # Initialize Kafka producer

    # Fetch NBA teams
    all_teams = teams.get_teams()
    logging.info(f"Retrieved {len(all_teams)} teams.")

    # Convert the list of dictionaries to a DataFrame
    df_teams = pd.DataFrame(all_teams)

    # Send each team's data to Kafka
    for _, team_row in df_teams.iterrows():
        team_dict = team_row.to_dict()
        team_json = json.dumps(team_dict)
        produce_message(producer, 'teams', team_json)

    # Close the producer after all messages are sent
    close_producer(producer)

# Define the DAG
with DAG(
    "kafka_retrieve_teams",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Retrieve NBA teams data and send to Kafka",
    schedule=None,
    start_date=datetime(2023, 12, 27),
    catchup=False,
) as dag:

    # Task to send teams data to Kafka
    kafka_teams_task = PythonOperator(
        task_id="send_teams_to_kafka",
        python_callable=send_teams_to_kafka
    )
