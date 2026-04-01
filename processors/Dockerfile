FROM python:3.10-slim

WORKDIR /app

# Suppression de librdkafka-dev car pika (RabbitMQ) n'en a pas besoin
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du nouveau nom de script
COPY processors/rabbitMQ_to_cassandra.py .

# Mise à jour des variables d'environnement pour RabbitMQ
ENV RABBIT_HOST="rabbitmq"
ENV CASSANDRA_HOST="cassandra-db"

# Commande de lancement mise à jour
CMD ["python", "-u", "rabbitMQ_to_cassandra.py"]