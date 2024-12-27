from nba_api.stats.endpoints import leaguegamefinder
from nba_api.live.nba.endpoints import boxscore
from confluent_kafka import Producer
import pandas as pd
import json

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:29092'  # Adjust if necessary
}
producer = Producer(producer_conf)

# Function to fetch all games for a given season
def fetch_season_games(season: str):
    """
    Fetch all games for a specific NBA season.
    :param season: String representing the NBA season, e.g., '2023-24'.
    :return: List of game IDs for the specified season.
    """
    game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
    games_data = game_finder.get_data_frames()[0]
    return games_data['GAME_ID'].tolist()

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

# Specify the season (adjust as needed)
season_to_fetch = '2023-24'  # Example: Change to '2022-23' for a different season

# Fetch all games for the specified season
game_ids = fetch_season_games(season_to_fetch)

if not game_ids:
    print(f"No games found for season {season_to_fetch}. Exiting.")
    exit(0)

# Process each game ID
for game_id in game_ids:
    print(f"Processing game ID: {game_id}")
    process_game_data(game_id)

# Ensure all messages are delivered
producer.flush()
print(f"All game data for season {season_to_fetch} have been sent to 'games' topic in Kafka.")
