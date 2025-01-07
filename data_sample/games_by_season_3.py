from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv2
import pandas as pd
import os
import time
from datetime import datetime, timedelta

# Function to get all game IDs for a specific date
def get_games_for_date(game_date: str):
    """
    Fetch all NBA game IDs for a given date.
    :param game_date: Date in 'YYYY-MM-DD' format.
    :return: List of game IDs.
    """
    scoreboard = scoreboardv2.ScoreboardV2(game_date=game_date)
    games = scoreboard.get_data_frames()[0]  # Retrieve game data
    
    if 'GAME_ID' not in games.columns:
        print(f"No games found for {game_date}.")
        return []
    
    return games['GAME_ID'].tolist()

# Function to process multiple games in a batch and save results
def process_games_batch(game_ids: list, season: str, output_dir: str):
    """
    Process multiple games in a batch and save data to CSV.
    :param game_ids: List of game IDs.
    :param season: Season for context.
    :param output_dir: Directory to save CSV.
    """
    all_data = []  # Store all data in memory for batch processing

    for game_id in game_ids:
        try:
            # Fetch boxscore data for the game
            boxscore = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id)
            df = boxscore.get_data_frames()[0]  # Get player stats dataframe
            
            if df.empty:
                print(f"No data for game {game_id}.")
                continue
            
            df['game_id'] = game_id
            df['season'] = season
            all_data.append(df)
        
        except Exception as e:
            print(f"Error processing game {game_id}: {e}")
    
    # Save batch data in one CSV write operation
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        csv_file = os.path.join(output_dir, f"{season.replace('-', '_')}_games.csv")
        
        if not os.path.exists(csv_file):
            final_df.to_csv(csv_file, index=False)
        else:
            final_df.to_csv(csv_file, mode='a', header=False, index=False)
        
        print(f"Processed and saved {len(all_data)} games.")

# Generate all dates for an NBA season (typically October to April)
def get_season_dates(season: str):
    """
    Generate all dates in the NBA season.
    :param season: String representing the season, e.g., '2023-24'.
    :return: List of dates in 'YYYY-MM-DD' format.
    """
    start_year = int(season[:4])  # Extract year
    start_date = datetime(start_year, 10, 1)  # Approximate NBA season start
    end_date = datetime(start_year + 1, 4, 15)  # Approximate regular season end

    return [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_date - start_date).days + 1)]

# Main execution
season_to_fetch = input("Enter NBA season (e.g., '2023-24'): ")
output_directory = "./data_sample/data/nba_season_data"
os.makedirs(output_directory, exist_ok=True)

dates = get_season_dates(season_to_fetch)

for game_date in dates:
    print(f"Fetching games for {game_date}...")
    game_ids = get_games_for_date(game_date)
    
    if game_ids:
        print(f"Processing {len(game_ids)} games for {game_date}...")
        process_games_batch(game_ids, season_to_fetch, output_directory)
    
    time.sleep(1)  # Prevent hitting API rate limits

print(f"All game data for {season_to_fetch} saved to {output_directory}.")
