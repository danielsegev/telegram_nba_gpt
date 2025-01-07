from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from nba_api.live.nba.endpoints import scoreboard, boxscore
from confluent_kafka import Producer
import pandas as pd
import json

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:29092'
}

def fetch_latest_games():
    producer = Producer(producer_conf)

    # Fetch today's scoreboard
    today_scoreboard = scoreboard.ScoreBoard()
    games = today_scoreboard.games.get_dict()

    # Filter for completed games (gameStatus = 3)
    finished_games = [g for g in games if g.get('gameStatus') == 3]

    if not finished_games:
        print("No finished games found for today. Exiting.")
        return

    for g in finished_games:
        game_id = g['gameId']
        print(f"Processing finished game: {game_id}")

        # Fetch the boxscore for this finished game
        live_game = boxscore.BoxScore(game_id=game_id)
        boxscore_data = live_game.get_dict()

        # Extract player data from home and away teams
        home_players = boxscore_data['game']['homeTeam'].get('players', [])
        away_players = boxscore_data['game']['awayTeam'].get('players', [])
        all_players = home_players + away_players

        # Flatten player data using pandas json_normalize
        if all_players:
            df = pd.json_normalize(all_players)
        else:
            df = pd.DataFrame()

        # Produce each player's record as a JSON message to the 'games' topic
        for _, player_row in df.iterrows():
            player_dict = player_row.to_dict()
            player_dict['game_id'] = game_id
            player_json = json.dumps(player_dict)
            producer.produce('games', value=player_json)

    # Ensure all messages are delivered
    producer.flush()
    print("All finished games data for today have been sent to 'games' topic in Kafka.")

# Define the DAG
with DAG(
    "fetch_latest_games",
    default_args=default_args,
    description="Fetch latest finished NBA games and send player stats to Kafka",
    schedule_interval="0 3 * * *",  # Runs daily at 3 AM
    start_date=datetime(2023, 12, 29),
    catchup=False,
) as dag:

    fetch_games_task = PythonOperator(
        task_id="fetch_latest_games_task",
        python_callable=fetch_latest_games,
    )

    spark_ingest_task = SparkSubmitOperator(
        task_id="spark_ingest_kafka_to_postgres",
        conn_id="spark_default",
        application="/opt/airflow/include/scripts/kafka_to_postgres_games.py",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0",
        executor_memory="2G",
        driver_memory="1G",
        name="Ingest Games Data from Kafka to Postgres",
        conf={
            "spark.executor.extraJavaOptions": "-Dlog4j.configuration=log4j.properties",
            "spark.driverEnv.SPARK_HOME": "/opt/spark",
            "spark.executorEnv.SPARK_HOME": "/opt/spark",
            "spark.driverEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
            "spark.executorEnv.PATH": "/opt/spark/bin:/usr/local/bin:/usr/bin:$PATH",
        },
    )

    fetch_games_task >> spark_ingest_task
