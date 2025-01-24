import json
import pandas as pd
from confluent_kafka import Producer
from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo


def fetch_and_send_players():
    conf = {'bootstrap.servers': 'kafka-learn:9092'}
    producer = Producer(conf)

    all_players = players.get_players()
    active_players = [p for p in all_players if p['is_active']]
    df_players = pd.DataFrame(active_players)
    extended_info_list = []

    for idx, row in df_players.iterrows():
        player_id = row['id']
        if idx % 10 == 0:
            print(f"Processing active player {idx+1}/{len(df_players)} with ID {player_id}")
        try:
            info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
            info_df = info.get_data_frames()[0]
            info_df = info_df.rename(columns={'PERSON_ID': 'id'})
            extended_info_list.append(info_df)
        except Exception as e:
            print(f"Error fetching data for player_id {player_id}: {e}")
            continue

    df_extended_info = pd.concat(extended_info_list, ignore_index=True) if extended_info_list else pd.DataFrame()
    df_merged = pd.merge(df_players, df_extended_info, on='id', how='left')

    try:
        for _, player_row in df_merged.iterrows():
            player_json = json.dumps(player_row.to_dict())
            producer.produce('players', value=player_json)
        producer.flush()
        print("✅ Players data successfully sent to Kafka topic 'players'.")
    except Exception as e:
        print(f"❌ Error producing to Kafka: {e}")


if __name__ == "__main__":
    fetch_and_send_players()
