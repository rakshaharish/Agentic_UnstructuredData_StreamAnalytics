import json
import kafka
from kafka import KafkaProducer

def send_mongo_event(tx_id, user_id, merchant_name, amount):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    payload = {
        "tx_id": tx_id,
        "user": {"id": user_id, "country": "US"},
        "merchant": {"name": merchant_name},
        "amount": float(amount)
    }
    producer.send('mongo-tx-topic', payload)
    producer.flush()
    producer.close()
    