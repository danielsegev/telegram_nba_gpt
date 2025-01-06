from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaConsumer
import psycopg2
import json

# Kafka configuration
KAFKA_TOPIC = "teams"
KAFKA_BOOTSTRAP_SERVERS = "kafka-learn:9092"

# PostgreSQL configuration
DB_CONFIG = {
    "dbname": "dwh",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}

# Function to insert data into PostgreSQL
def insert_into_postgres(records):
    """
    Insert records into the PostgreSQL database.
    """
    connection = psycopg2.connect(**DB_CONFIG)
    cursor = connection.cursor()

    # SQL query to insert data
    insert_query = """
    INSERT INTO public.nba_teams (team_id, abbreviation, city, full_name, state, year_founded)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (team_id) DO NOTHING;
    """

    for record in records:
        data = record.value.decode("utf-8")  # Decode the message
        data_dict = json.loads(data)  # Convert JSON string to Python dictionary
        cursor.execute(insert_query, (
            data_dict["team_id"],
            data_dict["abbreviation"],
            data_dict["city"],
            data_dict["full_name"],
            data_dict["state"],
            data_dict["year_founded"]
        ))

    connection.commit()
    cursor.close()
    connection.close()

# Function to consume messages from Kafka and load into PostgreSQL
def kafka_to_postgres():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="airflow-loader"
    )

    records = []
    for message in consumer:
        records.append(message)

        # Process in batches
        if len(records) >= 100:  # Batch size
            insert_into_postgres(records)
            records = []  # Clear the batch

    # Process remaining records
    if records:
        insert_into_postgres(records)

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="kafka_to_postgres_teams",
    default_args=default_args,
    description="Load data from Kafka 'teams' topic into PostgreSQL",
    schedule_interval=None,  # Run manually or configure as needed
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Define the PythonOperator task
    kafka_to_postgres_task = PythonOperator(
        task_id="kafka_to_postgres_task",
        python_callable=kafka_to_postgres,
    )

    kafka_to_postgres_task
