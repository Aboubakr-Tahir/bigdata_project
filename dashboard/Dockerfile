FROM python:3.10-slim

WORKDIR /app

# Suppression de librdkafka-dev
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dashboard/dashboard.py .

# On remplace KAFKA_BOOTSTRAP par RABBIT_HOST
ENV RABBIT_HOST="rabbitmq"

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]