
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

count = 0

print("Liczenie transakcji...")

for message in consumer:
    count += 1
    print("Liczba transakcji:", count)
