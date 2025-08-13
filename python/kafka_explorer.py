import streamlit as st
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

class OPTIONS:
    KAFKA_BROKER = "localhost:9092"
    HOST = "0.0.0.0"
    PORT = 5000

def peek_topics():
    admin = KafkaAdminClient(bootstrap_servers=OPTIONS.KAFKA_BROKER)
    topics = sorted(admin.list_topics())
    data = []

    for topic in topics:
        consumer = KafkaConsumer(bootstrap_servers=OPTIONS.KAFKA_BROKER, group_id=None, auto_commit=False)
        partitions = consumer.partitions_for_topic(topic) or []
        tps = [TopicPartition(topic, p) for p in partitions]
        end_offsets = consumer.end_offsets(tps)
        total_msgs = sum(end_offsets.values())
        consumer.close()
        data.append({"Topic": topic, "Queue Depth": total_msgs})

    admin.close()
    return data

st.set_page_config(page_title="Kafka Topics", layout="wide")
st.title("Kafka Topics & Queue Depths")

try:
    st.table(peek_topics())
except Exception as e:
    st.error(f"Error connecting to Kafka: {e}")
