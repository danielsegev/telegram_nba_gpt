from nba_api.stats.endpoints import leaguegamefinder
from datetime import datetime, timedelta
from confluent_kafka import Producer
import pandas as pd
import json
import time

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:29092'  # Adjust if necessary
}
producer = Producer(producer_conf)

# Get the current date and calculate the last 7 days
today = datetime.now()
start_date = today - timedelta(days=7)
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = today.strftime("%Y-%m-%d")

print(f"Fetching games from {start_date_str} to {end_date_str}")

try:
    # Fetch games within the date range using leaguegamefinder
    game_finder = leaguegamefinder.LeagueGameFinder(
        date_from_nullable=start_date_str,
        date_to_nullable=end_date_str
    )

    # Check if the response is valid
    games_df = game_finder.get_data_frames()[0]
    if games_df.empty:
        print(f"No games found between {start_date_str} and {end_date_str}. Exiting.")
        exit(0)

    # Filter for completed games (gameStatus = 3 equivalent)
    completed_games = games_df[games_df['WL'].notnull()]
    if completed_games.empty:
        print(f"No completed games found between {start_date_str} and {end_date_str}. Exiting.")
        exit(0)

    for _, game_row in completed_games.iterrows():
        game_id = game_row['GAME_ID']
        print(f"Processing finished game: {game_id}")

        # Convert row to dictionary and add game_date for context
        game_data = game_row.to_dict()
        game_data['game_date'] = game_row['GAME_DATE']

        # Serialize to JSON and send to Kafka
        game_json = json.dumps(game_data)
        producer.produce('games', value=game_json)
        time.sleep(0.1)  # Avoid overwhelming Kafka

except Exception as e:
    print(f"Error fetching or processing games: {e}")

# Ensure all messages are delivered
producer.flush()
print(f"All completed games data from {start_date_str} to {end_date_str} have been sent to 'games' topic in Kafka.")
