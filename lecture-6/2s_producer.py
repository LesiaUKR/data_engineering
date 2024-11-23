from kafka import KafkaProducer
from configs import kafka_config
import json
import uuid
import time
import random

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: str(v).encode('utf-8')
)

my_name = "oleksiy"
topic_name_in = f'{my_name}_spark_streaming_in'

# Відправлення даних у топік
for i in range(30):
    try:
        data = {
            "timestamp": time.time(),
            "value": random.randint(1, 100)
        }
        producer.send(topic_name_in, key=str(uuid.uuid4()), value=data)
        producer.flush()
        print(f"Message {i} sent successfully.")
        time.sleep(2)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()
