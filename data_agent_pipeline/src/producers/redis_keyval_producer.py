import json
from kafka import KafkaProducer

def send_redis_event(user_id, merchant_id, amount):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
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
