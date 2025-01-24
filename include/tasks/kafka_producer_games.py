import json
import logging
import pandas as pd
from confluent_kafka import Producer
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.live.nba.endpoints import boxscore


def fetch_and_process_games(season="2023-24", limit=100):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("fetch_and_process_games")

    producer = Producer({'bootstrap.servers': 'kafka-learn:9092'})

    # Fetch game data
    try:
        game_finder = leaguegamefinder.LeagueGameFinder(season_nullable=season)
        game_data = game_finder.get_data_frames()[0]
        game_ids = game_data['GAME_ID'].tolist()[:limit]
        game_dates = game_data.set_index('GAME_ID')['GAME_DATE'].to_dict()  # Map GAME_ID to GAME_DATE
    except Exception as e:
        logger.error(f"Error fetching game data: {e}")
        return

    # Process each game
    for game_id in game_ids:
        try:
            live_game = boxscore.BoxScore(game_id=game_id)
            boxscore_data = live_game.get_dict()

            home_players = boxscore_data['game']['homeTeam'].get('players', [])
            away_players = boxscore_data['game']['awayTeam'].get('players', [])
            all_players = home_players + away_players

            df = pd.json_normalize(all_players)

            for _, player_row in df.iterrows():
                player_dict = player_row.to_dict()
                player_dict['game_id'] = game_id
                player_dict['game_date'] = game_dates.get(game_id)  # Add game_date
                producer.produce('games', value=json.dumps(player_dict))

        except json.JSONDecodeError as json_err:
            logger.error(f"JSONDecodeError for game_id {game_id}: {json_err}")
            continue
        except Exception as e:
            logger.error(f"Error processing game_id {game_id}: {e}")
            continue

    producer.flush()
    logger.info("✅ Games data successfully sent to Kafka topic 'games'.")


if __name__ == "__main__":
    fetch_and_process_games()
