from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import logging

# Kafka Configuration
KAFKA_BROKERS = "kafka-learn:9092"
KAFKA_TOPIC = "games"
PARTITIONS = 3
REPLICATION_FACTOR = 1

# Initialize Kafka Admin Client
admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKERS})

def create_kafka_topic():
    """Ensures the Kafka topic is created before producing messages."""
    metadata = admin_client.list_topics(timeout=5)
    if KAFKA_TOPIC in metadata.topics:
        logging.info(f"✅ Kafka topic '{KAFKA_TOPIC}' already exists.")
    else:
        logging.info(f"⚠️ Topic '{KAFKA_TOPIC}' does not exist. Creating it...")
        new_topic = NewTopic(KAFKA_TOPIC, num_partitions=PARTITIONS, replication_factor=REPLICATION_FACTOR)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result()  # Block until the operation is complete
                logging.info(f"🎉 Kafka topic '{KAFKA_TOPIC}' created successfully!")
            except Exception as e:
                logging.error(f"❌ Failed to create topic '{KAFKA_TOPIC}': {e}")

# Ensure the topic exists before producing
create_kafka_topic()

# Kafka Producer Configuration
producer_conf = {"bootstrap.servers": KAFKA_BROKERS}
producer = Producer(producer_conf)

# ✅ Proceed with producing messages as usual...
