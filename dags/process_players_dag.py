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
import logging
import time
import random
from requests.exceptions import RequestException

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_player_data(player_id, max_retries=5, base_delay=5):
    """ Fetch player details using the NBA API with retries and backoff """
    retries = 0
    while retries < max_retries:
        try:
            logging.info(f"Fetching data for player_id {player_id}, attempt {retries+1}")
            info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=60)
            data = info.get_data_frames()

            if not data or len(data) == 0:
                logging.warning(f"Empty data returned for player_id {player_id}")
                return None

            logging.info(f"Successfully fetched data for player_id {player_id}")
            return data[0].rename(columns={'PERSON_ID': 'id'})

        except (RequestException, TimeoutError) as e:
            logging.warning(f"Request error fetching player_id {player_id}: {e}, retrying...")
            time.sleep(base_delay * (2 ** retries) + random.uniform(0, 1))  # Exponential backoff
            retries += 1
        except Exception as e:
            logging.error(f"Unexpected error fetching player_id {player_id}: {e}")
            return None  # Ensure it does not get retried infinitely

    logging.error(f"Failed to fetch data for player_id {player_id} after {max_retries} retries.")
    return None

def delivery_report(err, msg):
    """ Reports Kafka message delivery status """
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def fetch_and_send_players():
    """ Fetch all active players, enrich data, and send to Kafka topic """
    conf = {
        'bootstrap.servers': 'kafka-learn:9092',
        'linger.ms': 100,
        'batch.num.messages': 1000
    }
    producer = Producer(conf)

    all_players = players.get_players()
    df_players = pd.DataFrame(all_players)

    logging.info(f"Total players retrieved: {len(df_players)}")

    extended_info_list = []
    with ThreadPoolExecutor(max_workers=3) as executor:  # Reduced concurrency
        results = list(executor.map(fetch_player_data, df_players['id']))

    successful_calls = len([df for df in results if df is not None])
    logging.info(f"Successfully retrieved {successful_calls} player records.")

    extended_info_list = [df for df in results if df is not None]

    if extended_info_list:
        df_extended_info = pd.concat(extended_info_list, ignore_index=True)
        df_merged = pd.merge(df_players, df_extended_info, on='id', how='left')

        logging.info(f"Total players ready to be sent to Kafka: {len(df_merged)}")

        for _, player_row in df_merged.iterrows():
            player_json = json.dumps(player_row.to_dict())

            logging.debug(f"Sending player data to Kafka: {player_json}")

            try:
                producer.produce('players', value=player_json, callback=delivery_report)
            except Exception as e:
                logging.error(f"Failed to send player data to Kafka: {e}")

        logging.info("Flushing Kafka producer...")
        producer.flush()
        logging.info("All active player data sent to 'players' topic in Kafka.")
    else:
        logging.warning("No player data fetched successfully, skipping Kafka ingestion.")

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
        sql="TRUNCATE TABLE public.nba_players;",
    )

    send_players_task = PythonOperator(
        task_id="fetch_and_send_players",
        python_callable=fetch_and_send_players,
        execution_timeout=timedelta(minutes=30),  # ✅ Max execution time to avoid infinite runs
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
