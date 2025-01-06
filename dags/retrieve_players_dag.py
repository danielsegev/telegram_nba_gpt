from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from nba_api.stats.static import players
from nba_api.stats.endpoints import commonplayerinfo
import pandas as pd
import json
from confluent_kafka import Producer

def fetch_and_send_players():
    """
    Fetch active NBA players, enrich data using commonplayerinfo API, and send messages to Kafka topic 'players'.
    """
    conf = {'bootstrap.servers': 'kafka-learn:9092'}
    producer = Producer(conf)

    def delivery_report(err, msg):
        """Callback for message delivery confirmation."""
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

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

    num_messages = 0
    try:
        for _, player_row in df_merged.iterrows():
            player_json = json.dumps(player_row.to_dict())
            producer.produce('players', value=player_json, callback=delivery_report)
            num_messages += 1

        producer.flush()
        print(f"✅ {num_messages} messages sent to 'players' topic in Kafka.")
    except Exception as e:
        print(f"❌ Error producing to Kafka: {e}")

with DAG(
    "fetch_and_send_active_players",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2023, 12, 29),
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    description="Fetch active players from NBA API, enrich with details, and send to Kafka",
    schedule_interval=None,
    catchup=False,
) as dag:

    send_players_task = PythonOperator(
        task_id="fetch_and_send_players",
        python_callable=fetch_and_send_players,
    )
