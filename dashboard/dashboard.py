import streamlit as st
import pandas as pd
import pika
import json
import time
import os

# --- CONFIGURATION ---
rabbit_host = os.getenv('RABBIT_HOST', 'localhost')
EXCHANGE_NAME = 'iot_exchange'

st.set_page_config(page_title="RabbitMQ Live Dashboard", layout="wide")
st.title("🚀 Flux Temps Réel (Direct RabbitMQ)")

if 'data_buffer' not in st.session_state:
    st.session_state.data_buffer = pd.DataFrame(columns=[
        'timestamp', 'device_id', 'cpu_usage_percent', 'ram_available_mb', 'temperature_c'
    ])

@st.cache_resource
def get_rabbitmq_channel():
    # Connexion à RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbit_host, 
        credentials=pika.PlainCredentials('admin', 'supersecretpassword123')
    ))
    channel = connection.channel()
    
    # Déclaration de l'échangeur
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='fanout', durable=True)
    
    # Création d'une queue exclusive et temporaire avec un nom généré par RabbitMQ
    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    
    # Lier la queue à l'échangeur
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name)
    
    return channel, queue_name

channel, queue_name = get_rabbitmq_channel()

# --- INTERFACE UTILISATEUR ---
col1, col2, col3 = st.columns(3)
cpu_metric = col1.empty()
ram_metric = col2.empty()
temp_metric = col3.empty()

chart_cpu = st.empty()
chart_temp = st.empty()
data_table = st.empty()

# --- BOUCLE DE LECTURE ---
while True:
    # basic_get récupère un message s'il y en a un, sans bloquer (comme consumer.poll() de Kafka)
    method_frame, header_frame, body = channel.basic_get(queue=queue_name, auto_ack=True)

    if method_frame:
        try:
            new_data = json.loads(body.decode('utf-8'))
            new_row = pd.DataFrame([new_data])

            # Mise à jour du buffer de données
            st.session_state.data_buffer = pd.concat([st.session_state.data_buffer, new_row]).tail(50)
            df = st.session_state.data_buffer

            # Mise à jour des métriques
            cpu_metric.metric("CPU Live", f"{new_data['cpu_usage_percent']}%")
            ram_metric.metric("RAM Live", f"{new_data['ram_available_mb']} MB")
            temp_metric.metric("Température", f"{new_data['temperature_c']}°C")

            # Mise à jour des graphiques
            with chart_cpu.container():
                st.subheader("CPU en direct")
                st.line_chart(df.set_index('timestamp')['cpu_usage_percent'])

            with chart_temp.container():
                st.subheader("Température en direct")
                st.line_chart(df.set_index('timestamp')['temperature_c'])

            with data_table.container():
                st.dataframe(df.sort_values('timestamp', ascending=False), use_container_width=True)

        except Exception as e:
            st.error(f"Erreur de parsing : {e}")
    else:
        # Pause courte pour ne pas surcharger le CPU si la queue est vide
        time.sleep(0.1)