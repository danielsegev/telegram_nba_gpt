import subprocess
import logging
import time

def check_kafka_ready():
    """
    Checks if the Kafka broker is ready by listing available topics.
    Retries up to 5 times before failing.
    """
    for attempt in range(1, 6):
        try:
            logging.info("Checking if Kafka is ready (attempt %d)...", attempt)
            result = subprocess.run(
                ["kafka-topics", "--list", "--bootstrap-server", "kafka:9092"],
                check=True,
                capture_output=True,
                text=True
            )
            logging.info("Kafka topics available:\n%s", result.stdout)
            return True
        except subprocess.CalledProcessError as e:
            logging.warning("Kafka is not ready yet. Retrying in 5 seconds...")
            time.sleep(5)
    
    logging.error("Kafka did not become ready after multiple attempts.")
    raise RuntimeError("Kafka broker is not available.")

if __name__ == "__main__":
    check_kafka_ready()
