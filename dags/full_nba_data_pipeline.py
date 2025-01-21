from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta
import os
import sys
import logging
import json
import pandas as pd

from confluent_kafka import Producer
from nba_api.stats.static import teams, players
from nba_api.stats.endpoints import leaguegamefinder, commonplayerinfo
from nba_api.live.nba.endpoints import boxscore

# Ensure Airflow can find the scripts directory
dag_folder = os.path.dirname(os.path.abspath(__file__))
scripts_path = os.path.join(dag_folder, "scripts")
include_path = os.path.join(dag_folder, "include", "scripts")

for path in [scripts_path, include_path]:
    if path not in sys.path:
        sys.path.insert(0, path)

# Import database initialization scripts
from include.scripts.create_tables import create_database, create_tables

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email": ['your_email@example.com'],  # Replace with your email
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 6),
}

# Initialize DAG
with DAG(
    "nba_full_data_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    start_date=datetime(2025, 1, 1),
) as dag:

    # STEP 1: DATABASE INITIALIZATION
    def create_database_with_logging():
        logging.info("Starting database creation...")
        create_database()
        logging.info("Database creation completed.")

    def create_tables_with_logging():
        logging.info("Starting table creation...")
        create_tables()
        logging.info("Table creation completed.")

    initialize_postgres_db = PythonOperator(
        task_id="initialize_postgres_db",
        python_callable=create_database,
    )

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    initialize_postgres_db >> create_tables_task

    # STEP 2: DATA INGESTION INTO KAFKA (RUNS IN PARALLEL)
    def send_teams_to_kafka():
        producer = Producer({'bootstrap.servers': 'kafka-learn:9092'})
        all_teams = teams.get_teams()

        for _, team_row in pd.DataFrame(all_teams).iterrows():
            producer.produce('teams', value=json.dumps(team_row.to_dict()))

        producer.flush()

    kafka_retrieve_teams_task = PythonOperator(
        task_id="kafka_retrieve_teams",
        python_callable=send_teams_to_kafka
    )

    def fetch_and_send_players():
        """
        Fetch active NBA players, enrich data using commonplayerinfo API, and send messages to Kafka topic 'players'.
        """
        conf = {'bootstrap.servers': 'kafka-learn:9092'}
        producer = Producer(conf)

        def delivery_report(err, msg):
            """Callback for message delivery confirmation."""
            if err is not None:
                print(f"Message delivery failed: {err}")
            else:
                print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

        all_players = players.get_players()
        active_players = [p for p in all_players if p['is_active']]
        df_players = pd.DataFrame(active_players)
        extended_info_list = []

        for idx, row in df_players.iterrows():
            player_id = row['id']
            if idx % 10 == 0:
                print(f"Processing active player {idx+1}/{len(df_players)} with ID {player_id}")
            try:
                info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
                info_df = info.get_data_frames()[0]
                info_df = info_df.rename(columns={'PERSON_ID': 'id'})
                extended_info_list.append(info_df)
            except Exception as e:
                print(f"Error fetching data for player_id {player_id}: {e}")
                continue

        df_extended_info = pd.concat(extended_info_list, ignore_index=True) if extended_info_list else pd.DataFrame()
        df_merged = pd.merge(df_players, df_extended_info, on='id', how='left')

        num_messages = 0
        try:
            for _, player_row in df_merged.iterrows():
                player_json = json.dumps(player_row.to_dict())
                producer.produce('players', value=player_json, callback=delivery_report)
                num_messages += 1

            producer.flush()
            print(f"✅ {num_messages} messages sent to 'players' topic in Kafka.")
        except Exception as e:
            print(f"❌ Error producing to Kafka: {e}")

    fetch_and_send_active_players_task = PythonOperator(
        task_id="fetch_and_send_active_players",
        python_callable=fetch_and_send_players
    )

    def fetch_and_process_games(season="2023-24", limit=100):
        producer = Producer({'bootstrap.servers': 'kafka-learn:9092'})
        game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
        
        # Fetch game data including game_date
        game_data = game_finder.get_data_frames()[0]
        game_ids = game_data['GAME_ID'].tolist()[:limit]
        game_dates = game_data.set_index('GAME_ID')['GAME_DATE'].to_dict()  # Map GAME_ID to GAME_DATE

        for game_id in game_ids:
            live_game = boxscore.BoxScore(game_id=game_id)
            boxscore_data = live_game.get_dict()

            home_players = boxscore_data['game']['homeTeam'].get('players', [])
            away_players = boxscore_data['game']['awayTeam'].get('players', [])
            all_players = home_players + away_players

            df = pd.json_normalize(all_players)

            for _, player_row in df.iterrows():
                player_dict = player_row.to_dict()
                player_dict['game_id'] = game_id
                player_dict['game_date'] = game_dates.get(game_id)  # Add game_date
                producer.produce('games', value=json.dumps(player_dict))

        producer.flush()


    fetch_and_process_games_task = PythonOperator(
        task_id="fetch_and_process_games",
        python_callable=fetch_and_process_games,
    )

    create_tables_task >> [
        kafka_retrieve_teams_task,
        fetch_and_send_active_players_task,
        fetch_and_process_games_task
    ]

    # STEP 3: DATA PROCESSING WITH SPARK (RUNS IN PARALLEL)
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

    spark_kafka_to_postgres_teams = spark_submit_task(
        "spark_kafka_to_postgres_teams", "kafka_to_postgres_teams.py"
    )

    spark_kafka_to_postgres_players = spark_submit_task(
        "spark_kafka_to_postgres_players", "kafka_to_postgres_players.py"
    )

    spark_kafka_to_postgres_games = spark_submit_task(
        "spark_kafka_to_postgres_games", "kafka_to_postgres_games3.py"
    )

    kafka_retrieve_teams_task >> spark_kafka_to_postgres_teams
    fetch_and_send_active_players_task >> spark_kafka_to_postgres_players
    fetch_and_process_games_task >> spark_kafka_to_postgres_games
