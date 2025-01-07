from nba_api.stats.static import teams
import pandas as pd
from confluent_kafka import Producer
import json

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:29092'
}
producer = Producer(conf)

# Get a list of all NBA teams (both active and historical)
all_teams = teams.get_teams()

# Convert the list of dictionaries into a pandas DataFrame
df_teams = pd.DataFrame(all_teams)

# Produce each team's data as a JSON message to the 'teams' topic
for _, team_row in df_teams.iterrows():
    team_dict = team_row.to_dict()
    team_json = json.dumps(team_dict)
    producer.produce('teams', value=team_json)

# Flush all pending messages
producer.flush()

print("All teams data sent to 'teams' topic in Kafka.")
