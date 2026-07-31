import json
from kafka import KafkaProducer

def send_neo4j_event(user_id, merchant_id, amount):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    payload = {
        "payload": {
            "start": {"id": user_id},
            "end": {"id": merchant_id},
            "properties": {"amount": float(amount)}
        }
    }
    producer.send('neo4j-tx-topic', payload)
    producer.flush()
    producer.close()