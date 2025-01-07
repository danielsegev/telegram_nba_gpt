from nba_api.stats.endpoints import leaguegamefinder
from nba_api.live.nba.endpoints import boxscore
from confluent_kafka import Producer
import pandas as pd
import json
from datetime import datetime, timedelta

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:29092'
}
producer = Producer(producer_conf)

# Function to fetch all games for a given season until yesterday
def fetch_season_games_until_yesterday(season: str):
    """
    Fetch all games for a specific NBA season up until yesterday.
    :param season: String representing the NBA season, e.g., '2023-24'.
    :return: List of game IDs for the specified season until yesterday.
    """
    yesterday = datetime.today() - timedelta(days=1)
    game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
    games_data = game_finder.get_data_frames()[0]

    # Convert game date column to datetime format
    games_data['GAME_DATE'] = pd.to_datetime(games_data['GAME_DATE'])

    # Filter games until yesterday
    filtered_games = games_data[games_data['GAME_DATE'] < yesterday]

    print(f"Found {len(filtered_games)} games in season {season} until {yesterday.date()}.")
    return filtered_games['GAME_ID'].tolist()

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

        # Validate response before processing
        if not boxscore_data or 'game' not in boxscore_data:
            print(f"Skipping game {game_id}: No valid boxscore data found.")
            return

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

        # Produce each player's record as a JSON message to the 'games' topic
        for _, player_row in df.iterrows():
            player_dict = player_row.to_dict()
            player_dict['game_id'] = game_id
            player_json = json.dumps(player_dict)
            producer.produce('games', value=player_json)

        print(f"Game {game_id} processed successfully.")

    except Exception as e:
        print(f"Error processing game {game_id}: {e}")

# Specify the season
season_to_fetch = '2023-24'  

# Fetch all games for the specified season until yesterday
game_ids = fetch_season_games_until_yesterday(season_to_fetch)

if not game_ids:
    print(f"No games found for season {season_to_fetch} until yesterday. Exiting.")
    exit(0)

# Process each game ID
for game_id in game_ids:
    print(f"Processing game ID: {game_id}")
    process_game_data(game_id)

# Ensure all messages are delivered
producer.flush()
print(f"All game data for season {season_to_fetch} until yesterday have been sent to 'games' topic in Kafka.")
