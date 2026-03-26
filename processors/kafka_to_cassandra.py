import json
from confluent_kafka import Consumer, KafkaError
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime
import os

kafka_server = os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092')
cassandra_host = os.getenv('CASSANDRA_HOST', '127.0.0.1')

# --- CONFIGURATION ---
KAFKA_CONF = {
    'bootstrap.servers': kafka_server, # On est sur le Host, donc localhost
    'group.id': 'cassandra-consumer-group',
    'auto.offset.reset': 'earliest'
}
CASSANDRA_NODES = [cassandra_host]
TOPIC = 'test-iot'

def connect_cassandra():
    cluster = Cluster(CASSANDRA_NODES)
    session = cluster.connect('smart_city')
    return session

def main():
    # Connexion Cassandra
    print("[-] Connexion à Cassandra...")
    session = connect_cassandra()
    
    # Préparation de la requête (Prepared Statement pour la performance)
    insert_stmt = session.prepare("""
        INSERT INTO telemetry (device_id, timestamp, cpu_usage_percent, ram_available_mb, temperature_c)
        VALUES (?, ?, ?, ?, ?)
    """)

    # Connexion Kafka
    print("[-] Connexion à Kafka...")
    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([TOPIC])

    print(f"[*] En attente de messages sur le topic '{TOPIC}'...")

    try:
        while True:
            msg = consumer.poll(1.0) # Attend 1 seconde

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[!] Erreur Kafka: {msg.error()}")
                    break

            # --- LOGIQUE DE TRAITEMENT ---
            try:
                # 1. Parser le JSON
                data = json.loads(msg.value().decode('utf-8'))
                
                # 2. Convertir le timestamp ISO en objet datetime Python
                # Cassandra accepte les objets datetime directement
                ts_obj = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))

                # 3. Insertion dans Cassandra
                session.execute(insert_stmt, (
                    data['device_id'],
                    ts_obj,
                    float(data['cpu_usage_percent']),
                    float(data['ram_available_mb']),
                    float(data['temperature_c']) if data['temperature_c'] else None
                ))
                
                print(f"[OK] Message inséré : {data['device_id']} @ {data['timestamp']}")

            except Exception as e:
                print(f"[X] Erreur lors du processing du message: {e}")

    except KeyboardInterrupt:
        print("\n[!] Arrêt du consommateur...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
