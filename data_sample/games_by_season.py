from nba_api.stats.endpoints import leaguegamefinder
from nba_api.live.nba.endpoints import boxscore
import pandas as pd
import os

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

# Function to process game data and save to a CSV file
def process_game_data(game_id: str, season: str, output_dir: str):
    """
    Process boxscore data for a given game ID and save player stats to a CSV file.
    :param game_id: ID of the game to process.
    :param season: Season for context.
    :param output_dir: Directory where the CSV file will be saved.
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
            df['game_id'] = game_id  # Add game_id for context
            df['season'] = season  # Add season for context

            # Define output file path
            csv_file = os.path.join(output_dir, f"{season.replace('-', '_')}_games.csv")

            # Append data to CSV file
            if not os.path.exists(csv_file):
                df.to_csv(csv_file, index=False)
            else:
                df.to_csv(csv_file, mode='a', header=False, index=False)

            print(f"Game {game_id} processed successfully.")

        else:
            print(f"No player data available for game {game_id}.")

    except Exception as e:
        print(f"Error processing game {game_id}: {e}")

# Specify the season (adjust as needed)
season_to_fetch = input("Enter the NBA season to fetch (e.g., '2023-24'): ")
output_directory = "./nba_season_data"

# Create output directory if it does not exist
os.makedirs(output_directory, exist_ok=True)

# Fetch all games for the specified season
game_ids = fetch_season_games(season_to_fetch)

if not game_ids:
    print(f"No games found for season {season_to_fetch}. Exiting.")
    exit(0)

# Process each game ID
for game_id in game_ids:
    print(f"Processing game ID: {game_id}")
    process_game_data(game_id, season_to_fetch, output_directory)

print(f"All game data for season {season_to_fetch} have been saved to CSV in {output_directory}.")
