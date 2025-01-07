from nba_api.stats.endpoints import leaguegamefinder, boxscoretraditionalv2
import pandas as pd
import os
import time

# Function to fetch all game IDs for a given season
def fetch_season_games(season: str):
    """
    Fetch all games for a specific NBA season.
    :param season: String representing the NBA season, e.g., '2023-24'.
    :return: List of game IDs for the specified season.
    """
    game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
    games_data = game_finder.get_data_frames()[0]

    if 'GAME_ID' not in games_data.columns:
        print(f"Error: No game IDs found for season {season}.")
        return []

    return games_data['GAME_ID'].unique().tolist()

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
        boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
        boxscore_data = boxscore.get_data_frames()[0]  # Get player stats dataframe

        if boxscore_data.empty:
            print(f"No player stats available for game {game_id}.")
            return

        # Add context columns
        boxscore_data['game_id'] = game_id
        boxscore_data['season'] = season

        # Define output file path
        csv_file = os.path.join(output_dir, f"{season.replace('-', '_')}_games.csv")

        # Append data to CSV file
        if not os.path.exists(csv_file):
            boxscore_data.to_csv(csv_file, index=False)
        else:
            boxscore_data.to_csv(csv_file, mode='a', header=False, index=False)

        print(f"Game {game_id} processed successfully.")

    except Exception as e:
        print(f"Error processing game {game_id}: {e}")

# Specify the season (adjust as needed)
season_to_fetch = input("Enter the NBA season to fetch (e.g., '2022-23'): ")
output_directory = "./nba_season_data"

# Create output directory if it does not exist
os.makedirs(output_directory, exist_ok=True)

# Fetch all games for the specified season
game_ids = fetch_season_games(season_to_fetch)

if not game_ids:
    print(f"No games found for season {season_to_fetch}. Exiting.")
    exit(0)

# Process each game ID with a delay to avoid rate limits
for game_id in game_ids:
    print(f"Processing game ID: {game_id}")
    process_game_data(game_id, season_to_fetch, output_directory)
    time.sleep(1)  # Prevent hitting API rate limits

print(f"All game data for season {season_to_fetch} have been saved to CSV in {output_directory}.")
