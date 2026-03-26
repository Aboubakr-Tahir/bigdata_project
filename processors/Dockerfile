FROM python:3.10-slim

WORKDIR /app

# Installation des outils pour compiler confluent-kafka
RUN apt-get update && apt-get install -y gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# On copie le script
COPY processors/kafka_to_cassandra.py .

# On définit les variables pour Docker
ENV KAFKA_BOOTSTRAP="kafka-broker:29092"
ENV CASSANDRA_HOST="cassandra-db"

CMD ["python", "-u", "kafka_to_cassandra.py"]