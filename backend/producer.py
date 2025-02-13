from kafka import KafkaProducer
import json
import pandas as pd
import time

file_path = '/data/dataset_sismique.csv'
data = pd.read_csv(file_path)

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'topic1'

try:
    while True:
        for index, row in data.iterrows():
            message = {
                "timestamp": row['date'],
                "secousse": row['secousse'],
                "magnitude": row['magnitude'],
                "tension_entre_plaque": row['tension entre plaque']
            }
            producer.send(topic_name, value=message)
            print(f"Sent: {message}")
            time.sleep(1)
except KeyboardInterrupt:
    print("Stopping producer...")

producer.close()
