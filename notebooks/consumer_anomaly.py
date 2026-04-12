from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję anomalii prędkości...")

user_transactions = defaultdict(list)

WINDOW = 60
THRESHOLD = 3

for message in consumer:
    event = message.value
    
    user_id = event["user_id"]
    timestamp = datetime.fromisoformat(event["timestamp"])

    user_transactions[user_id].append(timestamp)

    user_transactions[user_id] = [
        t for t in user_transactions[user_id]
        if timestamp - t <= timedelta(seconds=WINDOW)
    ]

    if len(user_transactions[user_id]) > THRESHOLD:
        print(
            f"ALERT 🚨: {user_id} wykonał {len(user_transactions[user_id])} "
            f"transakcji w {WINDOW} sekund!"
        )