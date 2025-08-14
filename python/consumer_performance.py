#
# Kafka Consumer Performance Test Script (Open Source)
#
# This script uses the pure Python 'kafka-python' library to test the performance
# of a Kafka consumer. It connects to a Kafka topic, reads all available
# messages, and measures the total time taken and other metrics.
#
# To install the required libraries, run the following command:
# pip install kafka-python emoji
#
# This script assumes a running Kafka broker on 'localhost:9092' by default.
#

import logging
import os
import time
import sys
import emoji
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
# Import the TopicPartition namedtuple
from kafka.structs import TopicPartition

# --- Setup Logging with Emojis ---
# Configure the logging format to include emojis for a more visual experience.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("kafka").setLevel(logging.WARNING) # suppress kafka library logs

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

def kafka_consumer_performance_test(
    bootstrap_servers=['localhost:9092'],
    topic_name='perf-test',
    poll_timeout_ms=1000 # Timeout for each poll to check for new messages
):
    """
    Tests the performance of an open-source Kafka consumer.

    Args:
        bootstrap_servers (list): A list of Kafka broker connection strings.
        topic_name (str): The name of the Kafka topic to consume from.
        poll_timeout_ms (int): The timeout in milliseconds for each poll
                               to detect if the topic is empty.
    """
    log_info(f"{EMOJI_START} Starting Kafka consumer performance test...")

    # --- Configuration and Setup ---
    log_info(f"{EMOJI_CONFIG} Configuration:")
    log_info(f"  - Kafka Broker: {bootstrap_servers[0]}")
    log_info(f"  - Topic: {topic_name}")
    log_info(f"  - Poll Timeout: {poll_timeout_ms} ms")

    try:
        # Create a KafkaConsumer instance without a group_id. This allows us to
        # manually assign partitions without group coordination.
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False,
            # Setting a consumer_timeout_ms helps the loop terminate
            # after a period of inactivity.
            consumer_timeout_ms=poll_timeout_ms
        )
    except NoBrokersAvailable as e:
        log_error(f"Failed to connect to Kafka broker: {e}")
        log_error("Please ensure your Kafka broker is running and accessible.")
        sys.exit(1)
    except Exception as e:
        log_error(f"Failed to create Kafka consumer: {e}")
        sys.exit(1)
    
    # --- Ensure we read from the beginning of the topic ---
    # Manually get all partitions for the topic and assign them to the consumer.
    # This is the most reliable way to read all messages from the beginning
    # in a one-off performance test script.
    log_info(f"{EMOJI_INFO} Manually assigning partitions and seeking to the earliest offset...")
    partitions_for_topic = consumer.partitions_for_topic(topic_name)
    if partitions_for_topic:
        topic_partitions = [TopicPartition(topic_name, p) for p in partitions_for_topic]
        consumer.assign(topic_partitions)
        for tp in topic_partitions:
            consumer.seek(tp, 0)
    
    # --- Performance Measurement ---
    total_messages = 0
    total_size_bytes = 0
    start_time = time.time()
    
    log_info(f"{EMOJI_START} Consuming messages...")
    
    # The for loop iterates over all messages available from the earliest
    # offset until there are no more messages to consume. The consumer
    # will stop after the `consumer_timeout_ms` has passed without any
    # new messages.
    try:
        for message in consumer:
            total_messages += 1
            # The message value is a byte string.
            if message.value:
                total_size_bytes += len(message.value)
            
            # Print a progress indicator every 1000 messages
            if total_messages % 1000 == 0:
                sys.stdout.write(f"\r{EMOJI_INFO} Read {total_messages:,} messages...")
                sys.stdout.flush()

    except Exception as e:
        log_error(f"An error occurred during consumption: {e}")

    end_time = time.time()
    
    # --- Results and Metrics ---
    total_time_ms = (end_time - start_time) * 1000
    
    if total_messages > 0:
        per_message_time_ms = total_time_ms / total_messages
        messages_per_second = total_messages / (end_time - start_time)
        avg_message_size_kb = (total_size_bytes / total_messages) / 1024
        throughput_mbps = (total_size_bytes / (1024 * 1024)) / (end_time - start_time)
        total_data_mb = total_size_bytes / (1024 * 1024)
    else:
        log_warning("No messages were found in the topic.")
        log_info(f"{EMOJI_STOP} Test complete.")
        consumer.close()
        return

    sys.stdout.write("\n")
    log_info(f"{EMOJI_STOP} Test complete! Here are the results:")
    log_info(f"  - Total messages consumed: {total_messages:,}")
    log_info(f"  - Total data consumed: {total_data_mb:,.2f} MB")
    log_success(f"{EMOJI_CLOCK} Total consumption time: {total_time_ms:,.2f} ms")
    log_success(f"{EMOJI_CLOCK} Average time per message: {per_message_time_ms:,.4f} ms")
    log_success(f"{EMOJI_CLOCK} Average message size: {avg_message_size_kb:,.2f} KB")
    log_success(f"{EMOJI_CLOCK} Throughput: {messages_per_second:,.2f} messages/sec")
    log_success(f"{EMOJI_CLOCK} Throughput: {throughput_mbps:,.2f} MB/sec")

    consumer.close()

if __name__ == "__main__":
    os.system('cls')  # Clear console for a fresh start
    # The topic name should match the one used by the producer script.
    kafka_consumer_performance_test(
        topic_name='perf-test'
    )
