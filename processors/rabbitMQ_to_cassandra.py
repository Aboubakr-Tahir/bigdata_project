import pika
import json
import time
import os
from cassandra.cluster import Cluster
from datetime import datetime

# --- CONFIGURATION ---
RABBITMQ_HOST = os.getenv('RABBIT_HOST', 'rabbitmq')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'cassandra-db')
EXCHANGE_NAME = 'iot_exchange'

def connect_cassandra(retries=10, delay=10):
    cluster = Cluster([CASSANDRA_HOST])
    for i in range(retries):
        try:
            print(f"[-] Tentative Cassandra ({i+1}/{retries})...")
            session = cluster.connect()
            session.execute("CREATE KEYSPACE IF NOT EXISTS smart_city WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};")
            session.set_keyspace('smart_city')
            session.execute("""
                CREATE TABLE IF NOT EXISTS telemetry (
                    device_id text,
                    timestamp timestamp,
                    cpu_usage_percent float,
                    ram_available_mb float,
                    temperature_c float,
                    PRIMARY KEY (device_id, timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC);
            """)
            return session
        except Exception as e:
            print(f"[!] Erreur Cassandra: {e}")
            time.sleep(delay)
    raise Exception("Cassandra indisponible.")

def main():
    session = connect_cassandra()
    insert_stmt = session.prepare("INSERT INTO telemetry (device_id, timestamp, cpu_usage_percent, ram_available_mb, temperature_c) VALUES (?, ?, ?, ?, ?)")

    # Connexion RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=pika.PlainCredentials('admin', 'supersecretpassword123')))
    channel = connection.channel()

    # Déclaration de l'échangeur et d'une queue dédiée à Cassandra
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout', durable=True)
    result = channel.queue_declare(queue='cassandra_queue', durable=True)
    queue_name = result.method.queue
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)

    print(f"[*] En attente de messages sur RabbitMQ (Queue: {queue_name})...")

    def callback(ch, method, properties, body):
        try:
            data = json.loads(body)
            ts_obj = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            session.execute(insert_stmt, (
                data['device_id'], ts_obj,
                float(data['cpu_usage_percent']),
                float(data['ram_available_mb']),
                float(data['temperature_c']) if data.get('temperature_c') else None
            ))
            print(f"[OK] Inséré : {data['device_id']} @ {data['timestamp']}")
        except Exception as e:
            print(f"[X] Erreur processing : {e}")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()