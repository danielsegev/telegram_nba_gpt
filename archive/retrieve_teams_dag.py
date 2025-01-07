from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from nba_api.stats.static import teams
import pandas as pd
import json
from confluent_kafka import Producer

# Function to send NBA teams to Kafka
def send_teams_to_kafka():
    from confluent_kafka import Producer
    import logging

    logging.info("Initializing Kafka producer...")
    conf = {
        'bootstrap.servers': 'kafka-learn:9092'
    }
    producer = Producer(conf)

    logging.info("Fetching NBA teams...")
    all_teams = teams.get_teams()
    logging.info(f"Retrieved {len(all_teams)} teams.")

    for _, team_row in pd.DataFrame(all_teams).iterrows():
        team_json = json.dumps(team_row.to_dict())
        logging.info(f"Producing message: {team_json}")
        producer.produce('teams', value=team_json)

    logging.info("Flushing Kafka producer...")
    producer.flush()
    logging.info("All messages sent to Kafka.")


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
