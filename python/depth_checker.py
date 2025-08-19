#
# Kafka Topic Depth Checker
#
# This script uses the 'kafka-python' library to determine the total number of messages
# in a specified Kafka topic without consuming any data.
#
# To install the required libraries, run the following command:
# pip install kafka-python emoji
#
# This script assumes a running Kafka broker on 'localhost:9092' by default.
#

import logging
import sys
import emoji
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable

# --- Setup Logging with Emojis ---
# Configure the logging format to include emojis for a more visual experience.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define some emojis for different log levels
EMOJI_INFO = emoji.emojize(":information:")
EMOJI_SUCCESS = emoji.emojize(":check_mark_button:")
EMOJI_ERROR = emoji.emojize(":cross_mark:")
EMOJI_CONFIG = emoji.emojize(":gear:")
EMOJI_ANALYSIS = emoji.emojize(":magnifying_glass_tilted_right:")

def log_info(message):
    """Log an informational message with an emoji."""
    logging.info(f"{EMOJI_INFO} {message}")

def log_success(message):
    """Log a success message with an emoji."""
    logging.info(f"{EMOJI_SUCCESS} {message}")

def log_error(message):
    """Log an error message with an emoji."""
    logging.error(f"{EMOJI_ERROR} {message}")

def check_topic_depth(bootstrap_servers=['localhost:9092'],topic_name='perf-test'):
    """
    Checks the total number of messages (depth) in a Kafka topic.
    
    Args:
        bootstrap_servers (list): A list of Kafka broker connection strings.
        topic_name (str): The name of the Kafka topic to inspect.
    """
    log_info(f"Starting topic depth check for topic '{topic_name}'...")

    try:
        # Use KafkaConsumer to get topic metadata and offsets.
        # We specify group_id=None and enable_auto_commit=False to ensure we don't
        # accidentally affect any consumer groups or save our position.
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            # Setting a short timeout to fail fast if the broker is unavailable.
            consumer_timeout_ms=5000
        )
    except NoBrokersAvailable as e:
        log_error(f"Failed to connect to Kafka broker: {e}")
        log_error("Please ensure your Kafka broker is running and accessible.")
        sys.exit(1)
    except Exception as e:
        log_error(f"Failed to create Kafka consumer: {e}")
        sys.exit(1)
    
    # Check if the topic exists and get partition information
    log_info(f"{EMOJI_ANALYSIS} Fetching partition information...")
    try:
        partitions = consumer.partitions_for_topic(topic_name)
    except Exception as e:
        log_error(f"Failed to fetch partitions for topic '{topic_name}': {e}")
        log_error("This might mean the topic does not exist.")
        consumer.close()
        sys.exit(1)

    if not partitions:
        log_error(f"Topic '{topic_name}' does not exist or has no partitions.")
        consumer.close()
        sys.exit(1)

    log_info(f"{EMOJI_ANALYSIS} Calculating offsets for each partition...")
    
    # Create TopicPartition objects for each partition
    topic_partitions = [TopicPartition(topic_name, p) for p in partitions]
    
    # Get the latest offsets for each partition
    end_offsets = consumer.end_offsets(topic_partitions)
    
    # Get the earliest offsets for each partition
    earliest_offsets = consumer.beginning_offsets(topic_partitions)

    total_messages = 0
    partition_details = {}

    for tp in topic_partitions:
        try:
            earliest_offset = earliest_offsets.get(tp, 0)
            latest_offset = end_offsets.get(tp, 0)
            
            message_count = latest_offset - earliest_offset
            total_messages += message_count
            
            partition_details[tp.partition] = {
                'earliest_offset': earliest_offset,
                'latest_offset': latest_offset,
                'message_count': message_count
            }
        except Exception as e:
            log_error(f"Failed to get offsets for partition {tp.partition}: {e}")
            
    consumer.close()

    # --- Display Results ---
    log_success("Topic depth check complete!")
    log_info(f"{EMOJI_CONFIG} Topic: {topic_name}")
    log_info(f"{EMOJI_CONFIG} Broker: {bootstrap_servers[0]}")
    log_success(f"Total messages in topic: {total_messages:,}")

    # Display a breakdown per partition
    log_info("\n--- Partition Breakdown ---")
    if partition_details:
        for partition_id, details in partition_details.items():
            log_info(
                f"  Partition {partition_id}: {details['message_count']:,} messages "
                f"(Offsets: {details['earliest_offset']} to {details['latest_offset']})"
            )
    else:
        log_info("  No partition details to display.")
    return total_messages

if __name__ == "__main__":
    # The topic name should match the one used by your producer script.
    # By default, it will check the 'perf-test' topic.
    check_topic_depth(topic_name='perf-test')
