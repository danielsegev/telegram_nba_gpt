import logging
from confluent_kafka import Producer

# Kafka configuration
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:29092'  # Update this if your broker configuration changes
}

def initialize_producer():
    """
    Initialize a Kafka producer with the specified configuration.
    """
    try:
        logging.info("Initializing Kafka producer...")
        producer = Producer(KAFKA_CONF)
        return producer
    except Exception as e:
        logging.error("Failed to initialize Kafka producer", exc_info=True)
        raise

def produce_message(producer, topic, message):
    """
    Send a message to a Kafka topic.

    :param producer: Kafka Producer instance
    :param topic: Kafka topic to send the message to
    :param message: Message to be sent (string)
    """
    try:
        logging.info(f"Producing message to topic '{topic}': {message}")
        producer.produce(topic, value=message)
        producer.flush()  # Ensure the message is sent
        logging.info(f"Message successfully sent to topic '{topic}'")
    except Exception as e:
        logging.error(f"Failed to produce message to topic '{topic}'", exc_info=True)
        raise

def close_producer(producer):
    """
    Clean up and close the Kafka producer.
    """
    try:
        logging.info("Flushing and closing Kafka producer...")
        producer.flush()
        logging.info("Kafka producer closed successfully.")
    except Exception as e:
        logging.error("Error while closing Kafka producer", exc_info=True)
        raise
