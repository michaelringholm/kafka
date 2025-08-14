import logging
import threading
from flask import Flask, Response
from kafka import KafkaConsumer, KafkaProducer
import lxml.etree as ET
import routes_dac

# =========================
# CONFIGURATION
# =========================
class OPTIONS:
    KAFKA_BROKER = "localhost:9092"
    SOURCE_TOPIC = "quotes-topic"
    ROUTING_MATRIX = routes_dac.fetch_routing_matrix() 
    FLASK_HOST = "0.0.0.0"
    FLASK_PORT = 5001

# =========================
# LOGGING SETUP
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logging.getLogger("kafka").setLevel(logging.WARNING)
log = logging.getLogger(__name__)

# =========================
# FLASK APP FACTORY
# =========================
def create_app():
    app = Flask(__name__)

    # Init Kafka Producer
    app.producer = KafkaProducer(
        bootstrap_servers=OPTIONS.KAFKA_BROKER,
        value_serializer=lambda v: v.encode('utf-8')
    )

    # Start background consumer thread
    threading.Thread(target=consume_and_route, daemon=True).start()

    # Health endpoint
    @app.route("/health", methods=["GET"])
    def health():
        return Response("OK", status=200)

    return app

def consume_and_route():
        consumer = KafkaConsumer(
            OPTIONS.SOURCE_TOPIC,
            bootstrap_servers=OPTIONS.KAFKA_BROKER,
            auto_offset_reset="earliest",
            group_id="internal-router",
            value_deserializer=lambda v: v.decode('utf-8')
        )
        log.info("🚀 Internal Router started listening on topic '%s'", OPTIONS.SOURCE_TOPIC)

        for msg in consumer:
            log.info("📩 Received message from Kafka")
            try:
                doc = ET.fromstring(msg.value.encode('utf-8'))
                log.info("🔍 Parsed incoming XML")
            except ET.XMLSyntaxError as e:
                log.error("❌ Failed to parse XML: %s", e)
                continue

            matched = False
            for route in OPTIONS.ROUTING_MATRIX:
                match = doc.xpath(route["xpath"], namespaces=route["ns"])
                if match and match[0].text and match[0].text.strip() == route["value"]:
                    app.producer.send(route["dest_topic"], msg.value)
                    log.info("📤 Routed to topic '%s' because %s == '%s'",
                            route["dest_topic"], route["xpath"], route["value"])
                    matched = True

            if not matched:
                #log.warning("⚠️ No matching route found. Incoming XML:\n%s", msg.value.decode("utf-8"))
                log.warning("⚠️ No matching route found. Incoming XML:\n%s", msg.value)


            app.producer.flush()

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    app = create_app()
    app.run(host=OPTIONS.FLASK_HOST, port=OPTIONS.FLASK_PORT)
