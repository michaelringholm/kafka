import threading
from time import sleep
from flask import Flask, request, Response
from kafka import KafkaConsumer, KafkaProducer
import lxml.etree as ET

import adapter_config_reader
from logger_factory import create_logger

log = create_logger(__name__)
app = Flask(__name__)
interrupted = False
OPTIONS = adapter_config_reader.OPTIONS("config/stock_consumer_adapter_config.ini")

# =========================
# FLASK APP FACTORY
# =========================
def create_app():
    # Start background consumer thread
    threading.Thread(target=consume, daemon=False).start()
    return app

# Health endpoint
@app.route("/health", methods=["GET"])
def health():
    return Response("OK", status=200)

def consume():
        # Loop while not interrupted
        while not interrupted:
            consumer = KafkaConsumer(
                OPTIONS.KAFKA_TOPIC,
                bootstrap_servers=OPTIONS.KAFKA_BROKER,
                auto_offset_reset="earliest",
                group_id="stock_consumer",
                value_deserializer=lambda v: v.decode('utf-8')
            )
            log.info("🚀 Stock consumer started listening on topic '%s'", OPTIONS.KAFKA_TOPIC)

            for msg in consumer:
                log.info(f"📩 Received message from Kafka on topic [{OPTIONS.KAFKA_TOPIC}]")
                try:
                    doc = ET.fromstring(msg.value.encode('utf-8'))
                    log.info("🔍 Parsed incoming XML")
                except ET.XMLSyntaxError as e:
                    log.error("❌ Failed to parse XML: %s", e)
                    continue

                # Apply business logic
                log.info("🔍 Applying business logic...")
                # End of business logic
            log.info("⏳ Consumer thread finished processing messages batch. Sleeping until next messages arrive...")
            sleep(1)  # Sleep to avoid busy waiting

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    app = create_app()
    app.run(host=OPTIONS.FLASK_HOST, port=OPTIONS.FLASK_PORT, use_reloader=False)
