from nba_api.stats.static import teams
import pandas as pd

# Get a list of all NBA teams (both active and historical)
try:
    all_teams = teams.get_teams()
except Exception as e:
    print(f"Error fetching teams data: {e}")
    all_teams = []

# Convert the list of dictionaries into a pandas DataFrame
df_teams = pd.DataFrame(all_teams)

# Parameterize the file path or name
output_file = 'nba_teams.csv'  # You can change this to save to a different location or with a custom name

# Save the DataFrame to a CSV file
df_teams.to_csv(output_file, index=False)

print(f"All teams data saved to '{output_file}'.")
