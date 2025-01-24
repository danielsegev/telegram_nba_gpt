import json
import pandas as pd
from confluent_kafka import Producer
from nba_api.stats.static import teams


def send_teams_to_kafka():
    producer = Producer({'bootstrap.servers': 'kafka-learn:9092'})
    all_teams = teams.get_teams()

    for _, team_row in pd.DataFrame(all_teams).iterrows():
        producer.produce('teams', value=json.dumps(team_row.to_dict()))

    producer.flush()
    print("✅ Teams data successfully sent to Kafka topic 'teams'.")


if __name__ == "__main__":
    send_teams_to_kafka()
