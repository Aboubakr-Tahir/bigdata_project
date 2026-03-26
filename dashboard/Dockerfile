FROM python:3.10-slim

WORKDIR /app

# Installation des dépendances système pour confluent-kafka
RUN apt-get update && apt-get install -y gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du script dashboard
COPY dashboard/dashboard.py .

# Variables d'environnement pour Docker
ENV KAFKA_BOOTSTRAP="kafka-broker:29092"

# Port par défaut de Streamlit
EXPOSE 8501

# Commande pour lancer Streamlit
CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]