from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from nba_api.stats.static import teams
import pandas as pd
import json
from confluent_kafka import Producer

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Function to send NBA teams to Kafka
def send_teams_to_kafka():
    producer_conf = {'bootstrap.servers': 'kafka-learn:9092'}
    producer = Producer(producer_conf)

    all_teams = teams.get_teams()
    for _, team_row in pd.DataFrame(all_teams).iterrows():
        team_json = json.dumps(team_row.to_dict())
        producer.produce('teams', value=team_json)
    
    producer.flush()
    print("All team data sent to 'teams' topic in Kafka.")

# Define the DAG
with DAG(
    "fetch_teams_data",
    default_args=default_args,
    description="Retrieve NBA teams data and send to Kafka, then ingest into PostgreSQL",
    schedule_interval="0 0 1 9 *",  # Runs annually on September 1st at midnight
    start_date=datetime(2023, 9, 1),
    catchup=False,
) as dag:

    truncate_teams_table = PostgresOperator(
        task_id="truncate_nba_teams",
        postgres_conn_id="postgres_default",
        sql="TRUNCATE TABLE public.nba_teams;",
    )

    kafka_teams_task = PythonOperator(
        task_id="send_teams_to_kafka",
        python_callable=send_teams_to_kafka,
    )

    spark_ingest_teams_task = SparkSubmitOperator(
        task_id="spark_ingest_teams_to_postgres",
        conn_id="spark_default",
        application="/opt/airflow/include/scripts/kafka_to_postgres_teams.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0",
        executor_memory="2G",
        driver_memory="1G",
        name="Ingest Teams Data from Kafka to Postgres",
        conf={
            "spark.executor.extraJavaOptions": "-Dlog4j.configuration=log4j.properties",
            "spark.driverEnv.SPARK_HOME": "/opt/spark",
            "spark.executorEnv.SPARK_HOME": "/opt/spark",
            "spark.driverEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
            "spark.executorEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
        },
    )

    truncate_teams_table >> kafka_teams_task >> spark_ingest_teams_task
