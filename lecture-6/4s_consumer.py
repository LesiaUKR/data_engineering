from kafka import KafkaConsumer
import json
from configs import kafka_config

# Задаємо ім'я топіка
my_name = "oleksiy"
topic_name_out = f'{my_name}_spark_streaming_out'

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: v.decode('utf-8'),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_3'
)

# Підписка на вихідний топік
consumer.subscribe([topic_name_out])

print(f"Subscribed to topic '{topic_name_out}'")

# Обробка повідомлень
try:
    for message in consumer:
        print(f"Received message: {message.value} with key: {message.key}, partition {message.partition}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття Consumer після завершення
