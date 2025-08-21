# create venv for python
```
python -m venv venv
```

# activate venv
``` sh
source venv/bin/activate (Linux/Mac)
venv\Scripts\activate (Windows)
```

# install all required modules
```
pip install -r requirements.txt
```

# run kafka
```
docker-compose -f yaml/docker-compose.yml up -d
```
Make sure Kafka is running on localhost:9092

# run postgres
```
docker-compose -f yaml/docker-compose-postgres.yml up -d
```

# run zabbix
```
docker-compose -f yaml/docker-compose-zabbix-bootstrap.yml up -d
```

# check everything is running (OPTIONAL)
```
docker ps
```

# run the flow locally via python
``` sh
py stock_producer_adapter.py
py internal_routing_consumer.py
py stock_consumer_adapter.py
py sample_soap_client.py
```


# building adapters docker/podman image for flask apps
``` sh
docker build -f yaml/stock_producer_adapter.Dockerfile -t stock_producer_adapter .
docker build -f yaml/stock_consumer_adapter.Dockerfile -t stock_consumer_adapter .
# missing internal routing consumer for now...
```

# run the flow via flask adapter
``` ps1
$env:PYTHONPATH = "D:\github\kafka-demo\python"; $env:FLASK_APP = "stock_producer_adapter.py"; flask run
$env:PYTHONPATH = "D:\github\kafka-demo\python"; $env:FLASK_APP = "internal_routing_consumer.py"; flask run --no-reload
$env:PYTHONPATH = "D:\github\kafka-demo\python"; $env:FLASK_APP = "stock_consumer_adapter.py"; flask run
```

# run streamlit sandbox (OPTIONAL)
```
streamlit app run
```

# run all code tests
```
pytest
```
