from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.live.nba.endpoints import boxscore
from confluent_kafka import Producer
import pandas as pd
import json

def fetch_and_process_games(season="2023-24", limit=50):
    # Kafka producer configuration
    producer_conf = {
        'bootstrap.servers': 'kafka:9092'
    }
    producer = Producer(producer_conf)

    # Function to fetch all games for a given season
    def fetch_season_games(season: str, limit: int = 50):
        """
        Fetch a limited number of games for a specific NBA season.
        :param season: String representing the NBA season, e.g., '2023-24'.
        :param limit: Maximum number of games to fetch.
        :return: List of game IDs for the specified season (up to the limit).
        """
        game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
        games_data = game_finder.get_data_frames()[0]
        return games_data['GAME_ID'].tolist()[:limit]

    # Function to process game data and send to Kafka
    def process_game_data(game_id: str):
        """
        Process boxscore data for a given game ID and send player stats to Kafka.
        :param game_id: ID of the game to process.
        """
        try:
            # Fetch the boxscore for the game
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
                print(f"No player data available for game {game_id}.")
                return

            # Produce each player's record as a JSON message to the 'games' topic
            for _, player_row in df.iterrows():
                player_dict = player_row.to_dict()
                # Add game_id for context
                player_dict['game_id'] = game_id

                player_json = json.dumps(player_dict)
                producer.produce('games', value=player_json)

            print(f"Game {game_id} processed successfully.")

        except Exception as e:
            print(f"Error processing game {game_id}: {e}")

    # Fetch limited games for the specified season
    game_ids = fetch_season_games(season, limit)

    if not game_ids:
        print(f"No games found for season {season}. Exiting.")
        return

    # Process each game ID
    for game_id in game_ids:
        print(f"Processing game ID: {game_id}")
        process_game_data(game_id)

    # Ensure all messages are delivered
    producer.flush()
    print(f"Up to {limit} game data for season {season} have been sent to 'games' topic in Kafka.")

# Define the DAG
with DAG(
    "fetch_and_process_games",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Fetch game data from NBA API and send player stats to Kafka",
    schedule=None,  # Run manually or trigger on demand
    start_date=datetime(2023, 12, 29),
    catchup=False,
) as dag:

    process_games_task = PythonOperator(
        task_id="fetch_and_process_games_task",
        python_callable=fetch_and_process_games,
    )
