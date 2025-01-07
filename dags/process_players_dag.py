from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo
import pandas as pd
import json
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def fetch_player_data(player_id):
    """ Fetch player details using the NBA API """
    try:
        info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
        return info.get_data_frames()[0].rename(columns={'PERSON_ID': 'id'})
    except Exception as e:
        print(f"Error fetching data for player_id {player_id}: {e}")
        return None

def fetch_and_send_players():
    conf = {
        'bootstrap.servers': 'kafka-learn:9092',
        'linger.ms': 100,  # Wait up to 100ms before sending messages in a batch
        'batch.num.messages': 1000  # Send up to 1000 messages per batch
    }
    producer = Producer(conf)

    all_players = players.get_players()
    df_players = pd.DataFrame(all_players)

    extended_info_list = []
    with ThreadPoolExecutor(max_workers=10) as executor:  # Run 10 threads in parallel
        results = list(executor.map(fetch_player_data, df_players['id']))

    extended_info_list = [df for df in results if df is not None]

    df_extended_info = pd.concat(extended_info_list, ignore_index=True) if extended_info_list else pd.DataFrame()
    df_merged = pd.merge(df_players, df_extended_info, on='id', how='left')

    for _, player_row in df_merged.iterrows():
        player_json = json.dumps(player_row.to_dict())
        producer.produce('players', value=player_json)

    producer.flush()
    print("All active player data sent to 'players' topic in Kafka.")

# Define the DAG
with DAG(
    "fetch_players_data",
    default_args=default_args,
    description="Retrieve active NBA players data, send to Kafka, and ingest into PostgreSQL",
    schedule_interval="0 0 1 9 *",  # Runs annually on September 1st
    catchup=False,
) as dag:

    truncate_players_table = PostgresOperator(
        task_id="truncate_nba_players",
        postgres_conn_id="postgres_default",
        sql="TRUNCATE TABLE nba_players;",
    )

    send_players_task = PythonOperator(
        task_id="fetch_and_send_players",
        python_callable=fetch_and_send_players,
    )

    spark_ingest_players_task = SparkSubmitOperator(
        task_id="spark_ingest_players_to_postgres",
        conn_id="spark_default",
        application="/opt/airflow/include/scripts/kafka_to_postgres_players.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0",
        executor_memory="4G",
        driver_memory="2G",
        executor_cores=2,
        conf={
            "spark.executor.extraJavaOptions": "-Dlog4j.configuration=log4j.properties",
            "spark.driverEnv.SPARK_HOME": "/opt/spark",
            "spark.executorEnv.SPARK_HOME": "/opt/spark",
            "spark.driverEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
            "spark.executorEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
            "spark.sql.shuffle.partitions": "16",
        },
    )

    truncate_players_table >> send_players_task >> spark_ingest_players_task
