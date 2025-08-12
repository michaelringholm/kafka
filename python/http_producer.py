from flask import Flask, request, Response
from kafka import KafkaProducer
import lxml.etree as ET

app = Flask(__name__)

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "test-topic"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: v.encode('utf-8')
)

# Load your XSD schema for validation (same as previous example)
schema_doc = ET.parse("stockquote.xsd")
schema = ET.XMLSchema(schema_doc)

@app.route("/soap", methods=["POST"])
def soap_receive():
    soap_message = request.data
    try:
        doc = ET.fromstring(soap_message)
        schema.assertValid(doc)  # validate XSD
    except ET.DocumentInvalid as e:
        return Response(f"Invalid XML: {e}", status=400, mimetype="text/plain")

    # Forward raw SOAP message to Kafka topic
    producer.send(KAFKA_TOPIC, soap_message.decode('utf-8'))
    producer.flush()

    return Response("Message accepted", status=202, mimetype="text/plain")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
