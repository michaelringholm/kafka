import streamlit as st
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import time

from streamlit.delta_generator import DeltaGenerator
from consumer_performance import kafka_consumer_performance_test
from producer_perfomance import kafka_producer_performance_test
import sample_soap_client

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
    
# Callback for when a topic is selected, get the selected topic name and display the first message in that topic
def on_topic_select():
            # get selected_index from the dataframe selection
            selected_index = st.session_state.get("kafka_df2").get("selection").get("rows")[0]
            # selected_index is the index of the selected row in the dataframe
            topic_info = get_topic_info()
            if selected_index is not None and 0 <= selected_index < len(topic_info):
                selected = topic_info[selected_index]
                topic_name = selected['Topic']
                print(f"Selected topic: {topic_name}")
                try:
                    consumer = KafkaConsumer(
                        topic_name,
                        bootstrap_servers=KAFKA_BROKER,
                        auto_offset_reset='earliest',
                        enable_auto_commit=False,
                        max_poll_records=1,  # Only fetch one message
                        group_id=None,
                    )
                    print(f"Fetching messages from topic: {topic_name}")
                    # peek first message from a given topic
                    # This is a workaround since KafkaConsumer does not support peeking directly
                    # Get first message
                    first_message = next(consumer, None)
                    print(f"First message in topic '{topic_name}': {first_message}")
                    # Clean up
                    consumer.close()
                    # Display first message in the Streamlit app
                    if first_message:
                        st.info("Hi").json(first_message)
                        st.toast(first_message, icon="📥")
                    else:
                        st.warning(f"No messages found in topic '{topic_name}'")
                    consumer.close()
                except Exception as e:
                    st.error(f"Error fetching messages from topic '{topic_name}': {str(e)}")

def update_dataframe(kafka_df:DeltaGenerator, key:str) -> None:
    topic_info = get_topic_info()
    kafka_df.empty()  # Clear previous content
    if topic_info:
        # Use a unique key to avoid ID clash
        kafka_df.dataframe(
            topic_info,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select=on_topic_select,
            key=key

        )
    else:
        kafka_df.warning("No topics found or unable to connect to Kafka broker")  

def main():
    st.set_page_config(
        page_title="Sandbox UI",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Sandbox UI")
    kafka_df = st.empty()
    
    # Add refresh button
    if st.button("🔄 Refresh"):
        update_dataframe(kafka_df, "kafka_df..")
    
    # Get and display topic information
    update_dataframe(kafka_df, "kafka_df1")
    
    # Display last update time
    st.text(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Stack buttons vertically in a single column
    with st.container():
        soap_clicked = st.button("Send Sample SOAP Request")
        producer_clicked = st.button("Run Producer Performance Test")
        consumer_clicked = st.button("Run Consumer Performance Test")

    # Status label appears after the buttons
    status_label = st.empty()
    if soap_clicked:
        sample_soap_client.call_soap()
        status_label.write("Sample SOAP request sent!")
    elif producer_clicked:
        kafka_producer_performance_test.run_producer_perf_test()
        status_label.success("Producer performance test completed!")
    elif consumer_clicked:
        kafka_consumer_performance_test.run_consumer_perf_test()
        status_label.success("Consumer performance test completed!")      

    update_dataframe(kafka_df, "kafka_df2")

if __name__ == "__main__":
    main()