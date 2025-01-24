import subprocess
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_topic(topic_name, script_path):
    """
    Runs a Kafka producer script to ensure the specified Kafka topic is created.

    Args:
        topic_name (str): The Kafka topic name.
        script_path (str): The path to the producer script.
    """
    logger.info(f"Ensuring Kafka topic '{topic_name}' exists before ingestion...")
    try:
        result = subprocess.run(
            ["python", script_path],
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"Kafka topic '{topic_name}' creation output:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running script {script_path} for topic '{topic_name}': {e.stderr}")
        raise RuntimeError(f"Failed to create Kafka topic '{topic_name}'") from e
    except FileNotFoundError as e:
        logger.error(f"Script file not found: {script_path}")
        raise FileNotFoundError(f"Script path '{script_path}' is invalid or inaccessible") from e

def create_all_kafka_topics():
    """
    Ensures Kafka topics for 'games', 'teams', and 'players' exist.
    """
    topics = {
        "games": "/opt/airflow/include/scripts/kafka_games_producer.py",
        "teams": "/opt/airflow/include/scripts/kafka_teams_producer.py",
        "players": "/opt/airflow/include/scripts/kafka_players_producer.py"
    }

    for topic, script in topics.items():
        try:
            create_kafka_topic(topic, script)
        except Exception as e:
            logger.error(f"Failed to create Kafka topic '{topic}': {e}")
            raise

if __name__ == "__main__":
    # Allow execution for specific topics via CLI
    import argparse

    parser = argparse.ArgumentParser(description="Create Kafka topics via producer scripts.")
    parser.add_argument(
        "--topic",
        type=str,
        help="The name of the Kafka topic to create (optional). If not provided, all topics will be created.",
    )
    args = parser.parse_args()

    if args.topic:
        topic_mapping = {
            "games": "/opt/airflow/include/scripts/kafka_games_producer.py",
            "teams": "/opt/airflow/include/scripts/kafka_teams_producer.py",
            "players": "/opt/airflow/include/scripts/kafka_players_producer.py"
        }

        script_path = topic_mapping.get(args.topic)
        if script_path:
            create_kafka_topic(args.topic, script_path)
        else:
            logger.error(f"Invalid topic name provided: {args.topic}")
            raise ValueError(f"Invalid topic name: {args.topic}")
    else:
        create_all_kafka_topics()
