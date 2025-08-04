import threading
import json
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time
import logging
from kafka import KafkaProducer
import time

class OPTIONS:
    # Kafka connection
    BOOTSTRAP_SERVERS = 'localhost:9092'
    BROKER = BOOTSTRAP_SERVERS

    # Topic configuration
    TOPIC = 'test_topic'
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 1

    # Logging
    LOG_LEVEL = 'INFO'
    LOG_FORMAT = "🟢 PRODUCER | %(asctime)s | %(levelname)s: %(message)s"
    LOG_TIME_FORMAT = "%H:%M:%S"


# --- Helper Function for Topic Creation ---
def create_topic_if_not_exists(admin_client, topic_name, num_partitions, replication_factor):
    """
    Creates a new Kafka topic if it does not already exist.
    """
    try:
        # Check if the topic already exists
        if topic_name not in admin_client.list_topics():
            print(f"Topic '{topic_name}' not found. Attempting to create it...")
            topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
        else:
            print(f"Topic '{topic_name}' already exists.")
    except TopicAlreadyExistsError:
        # This can happen in a race condition, so we just continue.
        print(f"Topic '{topic_name}' already exists (caught TopicAlreadyExistsError).")
    except Exception as e:
        print(f"An error occurred while trying to create topic '{topic_name}': {e}")
        # Re-raise the exception if it's critical, or handle it as needed.
        raise


def setup_logging():
    # Configure logging
    #logging.basicConfig(
    #    level=getattr(logging, OPTIONS.LOG_LEVEL),
    #    format=OPTIONS.LOG_FORMAT,
    #    datefmt=OPTIONS.LOG_TIME_FORMAT
    #)

    logging.basicConfig(
        level=logging.INFO,
        format="🟢 PRODUCER | %(asctime)s | %(levelname)s: %(message)s",
        datefmt="%H:%M:%S"
    )

def produce():
    # Initialize producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    logging.info("Connected to Kafka broker at localhost:9092")

    # Send messages
    messages = [
    'Hello Kafka 👋'.encode('utf-8'),
    'Message 2 ✅'.encode('utf-8'),
    'Final message 🚀'.encode('utf-8')
    ]


    for msg in messages:
        producer.send(OPTIONS.TOPIC, msg)
        logging.info(f"Sent message: {msg.decode('utf-8')}")
        time.sleep(1)

    producer.flush()
    logging.info("All messages sent and flushed ✅")

from kafka import KafkaConsumer

def consume():
    consumer = KafkaConsumer(
        OPTIONS.TOPIC,
        bootstrap_servers=OPTIONS.BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='my-group',
        enable_auto_commit=True
    )
    logging.info(f"Consumer started for topic: {OPTIONS.TOPIC}")
    try:
        for message in consumer:
            msg_value = message.value.decode('utf-8')
            logging.info(f"Consumed message: {msg_value}")
    except Exception as e:
        logging.error(f"Error in consumer: {e}")
    finally:
        consumer.close()
        logging.info("Consumer closed.")



def old_produce():
    producer = KafkaProducer(bootstrap_servers=OPTIONS.KAFKA_BROKER)
    for i in range(10):
        msg = f'Message {i}'.encode('utf-8')
        producer.send(OPTIONS.TOPIC, msg)
        print(f'Produced: {msg.decode()}')
        time.sleep(1)
    producer.close()

#def consume():
#    consumer = KafkaConsumer(
#        TOPIC,
#        bootstrap_servers=KAFKA_BROKER,
#        auto_offset_reset='earliest',
#        group_id='my-group'
#    )
#    for msg in consumer:
#        print(f'Consumed: {msg.value.decode()}')

#def create_topic():
#    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
#    topic_list = [NewTopic(name=TOPIC, num_partitions=1, replication_factor=1)]
#    admin_client.create_topics(new_topics=topic_list, validate_only=False)
#    print(f'Topic {TOPIC} created.')
#    admin_client.close()

#if __name__ == '__main__':
    #t1 = threading.Thread(target=produce)
    #t2 = threading.Thread(target=consume)
    #t1.start()
    #t2.start()
    #t1.join()
    # Consumer runs indefinitely; you can stop it manually
    #create_topic()
    #produce()  # Uncomment to run producer
    #consume()  # Uncomment to run consumer
    # Note: Uncomment the threading part to run both producer and consumer concurrently

# --- Main Script Execution ---
if __name__ == "__main__":
    # --- Admin Client Setup and Topic Creation ---
    setup_logging()  # Set up logging before connecting to Kafka
    print(f"Connecting to Kafka Admin Client at {OPTIONS.BOOTSTRAP_SERVERS}...")
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=OPTIONS.BOOTSTRAP_SERVERS)
        create_topic_if_not_exists(admin_client, OPTIONS.TOPIC, OPTIONS.NUM_PARTITIONS, OPTIONS.REPLICATION_FACTOR)
        produce()  # Call the produce function to send messages
        consume()
        admin_client.close()
        print("Admin Client closed.")
    except Exception as e:
        print(f"Failed to connect to Kafka Admin Client: {e}")
        exit()
