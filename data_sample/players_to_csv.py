from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo
import pandas as pd

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

# Save the merged data to a CSV file
output_file = 'nba_players.csv'  # You can change this to save to a different location or with a custom name
df_merged.to_csv(output_file, index=False)

print(f"All players' extended data saved to '{output_file}'.")
