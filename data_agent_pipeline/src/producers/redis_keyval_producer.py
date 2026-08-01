import json
import os
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def send_redis_event(user_id, merchant_id, amount):
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    payload = {
        "user_id": user_id,
        "merchant_id": merchant_id,
        "amount": float(amount),
        "velocity_flag": "false"
    }
    producer.send('redis-tx-topic', payload)
    producer.flush()
    producer.close()
