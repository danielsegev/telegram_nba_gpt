from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo
import pandas as pd
import json
from confluent_kafka import Producer

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:29092'  # Use 'localhost:9092' if running on the host machine
}
producer = Producer(conf)

# Get all players
all_players = players.get_players()

# Convert to DataFrame
df_players = pd.DataFrame(all_players)

extended_info_list = []
for idx, row in df_players.iterrows():
    player_id = row['id']
    # Progress logging every 10 players
    if idx % 10 == 0:
        print(f"Processing player {idx+1}/{len(df_players)} with ID {player_id}")
    try:
        info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
        info_df = info.get_data_frames()[0]

        # Rename PERSON_ID to 'id' for a clean merge
        info_df = info_df.rename(columns={'PERSON_ID': 'id'})
        extended_info_list.append(info_df)
    except Exception as e:
        print(f"Error fetching data for player_id {player_id}: {e}")
        continue

# Combine all returned DataFrames
df_extended_info = pd.concat(extended_info_list, ignore_index=True) if extended_info_list else pd.DataFrame()

# Merge the basic player info with the extended info
df_merged = pd.merge(df_players, df_extended_info, on='id', how='left')

# Produce each player's data to Kafka 'players' topic
try:
    for _, player_row in df_merged.iterrows():
        # Convert row to dictionary and JSON
        player_dict = player_row.to_dict()
        player_json = json.dumps(player_dict)

        # Produce message to Kafka
        producer.produce('players', value=player_json)
    
    # Flush all messages at once after producing
    producer.flush()
    print("All players' extended data sent to 'players' topic in Kafka.")
except Exception as e:
    print(f"Error producing to Kafka: {e}")
