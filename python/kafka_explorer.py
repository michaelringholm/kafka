import streamlit as st
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import time

# Configuration
KAFKA_BROKER = "localhost:9092"
REFRESH_INTERVAL = 5  # seconds

def get_topic_info():
    """Get information about Kafka topics and their message counts."""
    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            client_id='streamlit-topic-viewer'
        )
        
        # Get list of topics
        topics = sorted(admin_client.list_topics())
        topic_info = []
        # Create consumer without group ID for metadata only
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            group_id=None,  # No consumer group needed
            enable_auto_commit=False
        )
        
        # Collect topic information
        
        for topic in topics:
            try:
                # Get partitions for topic
                partitions = consumer.partitions_for_topic(topic)
                if partitions:
                    # Create TopicPartition objects
                    tps = [TopicPartition(topic, p) for p in partitions]
                    
                    # Get end offsets (latest messages)
                    end_offsets = consumer.end_offsets(tps)
                    
                    # Calculate total messages
                    total_msgs = sum(end_offsets.values())
                    
                    topic_info.append({
                        "Topic": topic,
                        "Partitions": len(partitions),
                        "Total Messages": total_msgs
                    })
            except Exception as e:
                st.error(f"Error processing topic {topic}: {str(e)}")
                
        # Clean up
        consumer.close()
        admin_client.close()
        
        return topic_info
        
    except KafkaError as e:
        st.error(f"Kafka Error: {str(e)}")
        return []
    except Exception as e:
        st.error(f"Unexpected Error: {str(e)}")
        return []

def main():
    st.set_page_config(
        page_title="Kafka Topic Monitor",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Kafka Topic Monitor")
    
    # Add refresh button
    #if st.button("🔄 Refresh"):
    #    st.experimental_rerun()
    
    # Get and display topic information
    topic_info = get_topic_info()
    
    if topic_info:
        st.dataframe(
            topic_info,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.warning("No topics found or unable to connect to Kafka broker")
    
    # Display last update time
    st.text(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()