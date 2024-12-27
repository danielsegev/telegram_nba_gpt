from nba_api.live.nba.endpoints import scoreboard, boxscore
from confluent_kafka import Producer
import pandas as pd
import json

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:29092'  # Adjust if necessary
}
producer = Producer(producer_conf)

# Fetch today's scoreboard
today_scoreboard = scoreboard.ScoreBoard()
games = today_scoreboard.games.get_dict()

# Filter for completed games (gameStatus = 3)
finished_games = [g for g in games if g.get('gameStatus') == 3]

if not finished_games:
    print("No finished games found for today. Exiting.")
    exit(0)

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
    # Including the game_id in each message for traceability
    for _, player_row in df.iterrows():
        player_dict = player_row.to_dict()
        # Add game_id for context if desired
        player_dict['game_id'] = game_id

        player_json = json.dumps(player_dict)
        producer.produce('games', value=player_json)

# Ensure all messages are delivered
producer.flush()
print("All finished games data for today have been sent to 'games' topic in Kafka.")
