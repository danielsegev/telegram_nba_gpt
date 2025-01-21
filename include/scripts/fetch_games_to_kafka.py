from nba_api.stats.endpoints import leaguegamefinder
from nba_api.live.nba.endpoints import boxscore
from confluent_kafka import Producer
import pandas as pd
import json
from datetime import datetime

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'kafka-learn:9092'  # Adjust if necessary
}
producer = Producer(producer_conf)

# Number of partitions (set based on Kafka topic configuration)
NUM_PARTITIONS = 3  # Adjust this according to your Kafka topic setup

# Function to fetch all games for the given season and their dates
def fetch_season_games(season: str):
    """
    Fetch all games for a specific NBA season along with their game dates.
    :param season: String representing the NBA season, e.g., '2024-25'.
    :return: List of tuples (game_id, game_date) for the specified season.
    """
    game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
    games_data = game_finder.get_data_frames()[0]

    # Convert game dates to proper format
    games_data['GAME_DATE'] = pd.to_datetime(games_data['GAME_DATE']).dt.strftime('%Y-%m-%d')

    return list(zip(games_data['GAME_ID'], games_data['GAME_DATE']))

# Function to determine Kafka partition
def get_partition(game_id: str):
    """
    Determine the partition for a given game_id using hash function.
    :param game_id: ID of the game to process.
    :return: Partition number.
    """
    return hash(game_id) % NUM_PARTITIONS  # Distributes data across partitions

# Function to process game data and send to Kafka
def process_game_data(game_id: str, game_date: str):
    """
    Process boxscore data for a given game ID and send player stats to Kafka.
    :param game_id: ID of the game to process.
    :param game_date: Date the game was played.
    """
    try:
        # Fetch the boxscore for the game
        live_game = boxscore.BoxScore(game_id=game_id)
        boxscore_data = live_game.get_dict()

        # Extract player data from home and away teams
        home_team = boxscore_data['game'].get('homeTeam', {})
        away_team = boxscore_data['game'].get('awayTeam', {})

        home_players = home_team.get('players', [])
        away_players = away_team.get('players', [])
        all_players = home_players + away_players

        if not all_players:
            print(f"Skipping game {game_id}: No player data available.")
            return

        # Flatten player data using pandas json_normalize
        df = pd.json_normalize(all_players)

        # Add game_id and game_date for context
        df['game_id'] = game_id
        df['game_date'] = game_date

        # Determine partition for this game
        partition = get_partition(game_id)

        # Produce each player's record as a JSON message to the 'games' topic
        for _, player_row in df.iterrows():
            player_dict = player_row.to_dict()
            player_json = json.dumps(player_dict)
            producer.produce('games', value=player_json, partition=partition)

        print(f"Game {game_id} from {game_date} processed successfully (Partition {partition}).")

    except Exception as e:
        print(f"Error processing game {game_id}: {e}")

# Specify the current season (2024-25)
season_to_fetch = '2024-25'

# Fetch all games for the specified season
game_data = fetch_season_games(season_to_fetch)

if not game_data:
    print(f"No games found for season {season_to_fetch}. Exiting.")
    exit(0)

# Process each game ID along with its date
for game_id, game_date in game_data:
    print(f"Processing game ID: {game_id} from {game_date}")
    process_game_data(game_id, game_date)

# Ensure all messages are delivered
producer.flush()
print(f"All game data for season {season_to_fetch} have been sent to 'games' topic in Kafka.")
