# create venv for python
python -m venv venv

# activate venv
source venv/bin/activate (Linux/Mac)
venv\Scripts\activate (Windows)

create python venvironment and install kafka-python
pip install kafka-python

# Download and run Kafka locally on docker
# docker run -d --name kafka -p 9092:9092 \
#   -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
#   -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT \
#   -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
#   -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
#   wurstmeister/kafka:latest

# run kafka
# docker-compose up -d
docker-compose -f docker-compose.yml up -d
Make sure Kafka is running on localhost:9092

# run postgres
docker-compose -f docker-compose-postgres.yml up -d

# check it is running
docker ps

run the app

# build docker/podman image for flask app
docker build -f soap_flask_adapter.Dockerfile -t soap-flask-adapter .

# run soap flask adapter
$env:FLASK_APP = "soap_flask_adapter.py" | flask run
$env:PYTHONPATH = "D:\github\kafka-demo\python"; $env:FLASK_APP = "internal_routing_consumer.py"; flask run --no-reload

# run streamlit app
