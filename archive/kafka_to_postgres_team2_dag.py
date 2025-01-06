from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
import psycopg2
import json

# PostgreSQL connection configuration
db_config = {
    "dbname": "dwh",
    "user": "airflow",
    "password": "airflow",
    "host": "192.168.64.1",  # Replace with your PostgreSQL host IP
    "port": "5432"
}

# Kafka configuration
kafka_topic = "teams"
kafka_bootstrap_servers = "kafka-learn:9092"

def consume_and_load_to_postgres():
    """
    Consume messages from the Kafka topic and load them into the PostgreSQL database.
    Terminates after processing all available messages.
    """
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset="earliest",  # Start from the earliest messages
        group_id="nba_teams_loader",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True
    )

    insert_query = """
    INSERT INTO nba_teams (team_id, abbreviation, city, full_name, state, year_founded)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (team_id) DO NOTHING;  -- Prevent duplicate inserts
    """

    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Poll messages for a fixed duration (e.g., 10 seconds) and exit if no new messages
        timeout_seconds = 10
        end_time = datetime.utcnow() + timedelta(seconds=timeout_seconds)

        for message in consumer:
            team = message.value
            print(f"Consuming message: {team}")

            data_tuple = (
                team.get("id"),
                team.get("abbreviation"),
                team.get("city"),
                team.get("full_name"),
                team.get("state"),
                team.get("year_founded")
            )

            # Insert data into PostgreSQL
            try:
                cursor.execute(insert_query, data_tuple)
                conn.commit()
                print(f"Inserted team into PostgreSQL: {data_tuple}")
            except Exception as e:
                print(f"Error inserting data into PostgreSQL: {e}")
                conn.rollback()

            # Exit if timeout is reached
            if datetime.utcnow() > end_time:
                print("Timeout reached. Exiting consumer.")
                break

    except Exception as e:
        print(f"Error connecting to PostgreSQL or consuming Kafka messages: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        consumer.close()

# Define the Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "kafka_to_postgres_no_spark",
    default_args=default_args,
    description="Consume Kafka messages and load to PostgreSQL without Spark",
    schedule=None,  # Manually triggered
    start_date=datetime(2023, 12, 27),
    catchup=False,
) as dag:

    kafka_to_postgres_task = PythonOperator(
        task_id="consume_and_load_to_postgres",
        python_callable=consume_and_load_to_postgres
    )
