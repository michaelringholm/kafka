#
# Kafka Producer Performance Test Script (Open Source)
#
# This script uses the pure Python 'kafka-python' library to test the performance
# of a Kafka producer. It sends a specified number of messages of a fixed size
# to a Kafka topic and measures the total time taken.
#
# To install the required libraries, run the following command:
# pip install kafka-python emoji
#
# This script requires a running Kafka broker. The script assumes it is
# running on 'localhost:9092' by default.
#

import logging
import time
import sys
import emoji
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaTimeoutError

# --- Setup Logging with Emojis ---
# Configure the logging format to include emojis for a more visual experience.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define some emojis for different log levels
EMOJI_INFO = emoji.emojize(":information:")
EMOJI_SUCCESS = emoji.emojize(":check_mark_button:")
EMOJI_WARNING = emoji.emojize(":warning:")
EMOJI_ERROR = emoji.emojize(":cross_mark:")
EMOJI_START = emoji.emojize(":rocket:")
EMOJI_STOP = emoji.emojize(":chequered_flag:")
EMOJI_CLOCK = emoji.emojize(":hourglass_done:")
EMOJI_CONFIG = emoji.emojize(":gear:")

def log_info(message):
    """Log an informational message with an emoji."""
    logging.info(f"{EMOJI_INFO} {message}")

def log_success(message):
    """Log a success message with an emoji."""
    logging.info(f"{EMOJI_SUCCESS} {message}")

def log_warning(message):
    """Log a warning message with an emoji."""
    logging.warning(f"{EMOJI_WARNING} {message}")

def log_error(message):
    """Log an error message with an emoji."""
    logging.error(f"{EMOJI_ERROR} {message}")

def kafka_producer_performance_test(
    bootstrap_servers=['localhost:9092'],
    topic_name='perf-test',
    num_messages=10000,
    message_size_kb=1
):
    """
    Tests the performance of an open-source Kafka producer.

    Args:
        bootstrap_servers (list): A list of Kafka broker connection strings.
        topic_name (str): The name of the Kafka topic to produce to.
        num_messages (int): The number of messages to produce.
        message_size_kb (int): The size of each message in kilobytes.
    """
    log_info(f"{EMOJI_START} Starting Kafka producer performance test...")
    
    # --- Configuration and Setup ---
    message_size_bytes = message_size_kb * 1024
    sample_message = 'X' * message_size_bytes
    
    log_info(f"{EMOJI_CONFIG} Configuration:")
    log_info(f"  - Kafka Broker: {bootstrap_servers[0]}")
    log_info(f"  - Topic: {topic_name}")
    log_info(f"  - Number of messages: {num_messages:,}")
    log_info(f"  - Message size: {message_size_kb:,} KB ({message_size_bytes:,} bytes)")
    
    # Check if the topic exists and create it if not, using the AdminClient
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        
        # 'kafka-python' doesn't have a simple list_topics, so we check for the
        # topic when trying to create it.
        try:
            admin_client.create_topics([NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1
            )], validate_only=False)
            log_success(f"Topic '{topic_name}' created successfully.")
        except TopicAlreadyExistsError:
            log_success(f"Topic '{topic_name}' already exists.")
        except Exception as e:
            log_error(f"Failed to create topic '{topic_name}': {e}")
            sys.exit(1)
        finally:
            admin_client.close()
            
    except KafkaTimeoutError as e:
        log_error(f"Failed to connect to Kafka broker to check/create topic: {e}")
        log_error("Please ensure your Kafka broker is running and accessible.")
        sys.exit(1)

    # Kafka Producer configuration
    producer_config = {
        'bootstrap_servers': bootstrap_servers,
        # Linger_ms allows more messages to be batched together
        'linger_ms': 10,
        # The 'acks' setting controls message durability vs. performance.
        # acks=0 (default) provides highest throughput, lowest durability.
        # acks=1 for leader acknowledgment.
        # acks='all' for all in-sync replica acknowledgments (highest durability).
        'acks': 'all'
    }

    try:
        # KafkaProducer for kafka-python library expects a list for bootstrap_servers
        p = KafkaProducer(**producer_config)
    except Exception as e:
        log_error(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

    # --- Performance Measurement ---
    start_time = time.time()
    
    log_info(f"{EMOJI_START} Producing messages...")
    
    for i in range(num_messages):
        # The send() method is asynchronous. It returns a future object.
        # We don't need to wait for each message in a performance test.
        try:
            p.send(topic_name, key=f"key-{i}".encode('utf-8'), value=sample_message.encode('utf-8'))
        except Exception as e:
            log_error(f"Failed to produce message {i}: {e}")
            break
            
    # Wait for any outstanding messages to be delivered
    log_info(f" Flushing any remaining messages...")
    p.flush(timeout=30)
    
    end_time = time.time()
    
    # --- Results and Metrics ---
    total_time_ms = (end_time - start_time) * 1000
    if num_messages > 0:
        per_message_time_ms = total_time_ms / num_messages
        messages_per_second = num_messages / (end_time - start_time)
        data_sent_mb = (num_messages * message_size_bytes) / (1024 * 1024)
        throughput_mbps = data_sent_mb / (end_time - start_time)
    else:
        per_message_time_ms = 0
        messages_per_second = 0
        data_sent_mb = 0
        throughput_mbps = 0

    log_info(f"{EMOJI_STOP} Test complete! Here are the results:")
    log_info(f"  - Total messages produced: {num_messages:,}")
    log_info(f"  - Total data sent: {data_sent_mb:,.2f} MB")
    log_success(f"{EMOJI_CLOCK} Total production time: {total_time_ms:,.2f} ms")
    log_success(f"{EMOJI_CLOCK} Average time per message: {per_message_time_ms:,.4f} ms")
    log_success(f"{EMOJI_CLOCK} Throughput: {messages_per_second:,.2f} messages/sec")
    log_success(f"{EMOJI_CLOCK} Throughput: {throughput_mbps:,.2f} MB/sec")

    p.close()

if __name__ == "__main__":
    # Example usage:
    # Test with 10,000 messages of 1 KB each
    kafka_producer_performance_test(
        num_messages=10000,
        message_size_kb=10
    )

    # You can change the values for different tests
    # kafka_producer_performance_test(
    #    num_messages=100000,
    #    message_size_kb=5
    # )
    # Note: Use a larger number of messages for more accurate results.
