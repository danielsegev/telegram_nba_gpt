from confluent_kafka import Producer

def get_kafka_producer():
    """Returns a Kafka producer with predefined settings."""
    conf = {
        'bootstrap.servers': 'localhost:29092'  # Adjust if necessary
    }
    return Producer(conf)
