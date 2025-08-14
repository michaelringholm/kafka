from flask import Flask, request, Response
from kafka import KafkaProducer
import lxml.etree as ET
import logging
from datetime import datetime

# -----------------------------------
# OPTIONS CONFIG CLASS
# -----------------------------------
class OPTIONS:
    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "quotes-topic"
    XSD_SCHEMA_PATH = "sample_soap_schema.xsd"
    XPATH_EXPR = "//m:StockName"  # Example: find StockName element
    NAMESPACES = {"m": "https://www.example.org/stock"}  # Adjust to match your XML
    HOST = "0.0.0.0"
    PORT = 5000


# -----------------------------------
# LOGGING SETUP
# -----------------------------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
# Silence kafka-python's noisy broker connection logs
logging.getLogger("kafka").setLevel(logging.WARNING)
log = logging.getLogger(__name__)


app = Flask(__name__)
log = logging.getLogger(__name__)

def init_schema():
    schema_doc = ET.parse(OPTIONS.XSD_SCHEMA_PATH)
    app.schema = ET.XMLSchema(schema_doc)

@app.before_request
def create_app():
    log.info("🚀 Initializing Flask SOAP adapter on before_request signal...")
    init_schema()
    app.producer = KafkaProducer(
        bootstrap_servers=OPTIONS.KAFKA_BROKER,
        value_serializer=lambda v: v.encode("utf-8"),
        # enable_idempotence=True, only guarantees exactly-once delivery for a single producer instance to a single partition.
        # It does not guarantee idempotency across multiple producers or across multiple partitions.
        # If you use multiple producers or send to multiple partitions, duplicates may still occur.
        enable_idempotence=True # Ensures no duplicate messages are sent within the same prodcer session for a given partition
    )
    #schema_doc = ET.parse(OPTIONS.XSD_SCHEMA_PATH)
    #app.schema = ET.XMLSchema(schema_doc)


# -----------------------------------
# ROUTES
# -----------------------------------
@app.route("/soap", methods=["POST"])
def soap_receive():
    log.info("📩 Received SOAP request")
    soap_message = request.data

    try:
        # Parse XML
        doc = ET.fromstring(soap_message)
        log.info("🔍 Parsed incoming XML")

        # Validate XML
        app.schema.assertValid(doc)
        log.info("✅ XML validated against schema")

        # Extract specific value with XPath
        extracted_values = doc.xpath(OPTIONS.XPATH_EXPR, namespaces=OPTIONS.NAMESPACES)
        if extracted_values:
            log.info(f"📄 Extracted via XPath '{OPTIONS.XPATH_EXPR}': {extracted_values[0].text}")
        else:
            log.warning("⚠️ No match found for XPath expression")

        # Forward to Kafka
        app.producer.send(OPTIONS.KAFKA_TOPIC, soap_message.decode('utf-8'))
        app.producer.flush()
        log.info(f"📤 Posted to Kafka topic '{OPTIONS.KAFKA_TOPIC}' at {datetime.now()}")

        return Response("Message accepted", status=202, mimetype="text/plain")

    except ET.DocumentInvalid as e:
        log.error(f"❌ XML validation failed: {e}")
        return Response(f"Invalid XML: {e}", status=400, mimetype="text/plain")

    except ET.XMLSyntaxError as e:
        log.error(f"❌ XML syntax error: {e}")
        return Response(f"Malformed XML: {e}", status=400, mimetype="text/plain")

    except Exception as e:
        log.error(f"💥 Unexpected error: {e}")
        return Response(f"Internal Server Error: {e}", status=500, mimetype="text/plain")


# -----------------------------------
# ENTRY POINT
# -----------------------------------
if __name__ == "__main__":
    log.info(f"🚀 Starting Flask SOAP adapter on {OPTIONS.HOST}:{OPTIONS.PORT}")
    create_app()
    app.run(host=OPTIONS.HOST, port=OPTIONS.PORT)
