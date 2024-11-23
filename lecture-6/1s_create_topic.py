from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Визначення нового топіку
my_name = "oleksiy"
topic_name_in = f'{my_name}_spark_streaming_in'
topic_name_out = f'{my_name}_spark_streaming_out'
num_partitions = 2
replication_factor = 1

new_topics = [
    NewTopic(name=topic_name_in, num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(name=topic_name_out, num_partitions=num_partitions, replication_factor=replication_factor)
]

# Створення нових топіків
try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"Topics '{topic_name_in}' and '{topic_name_out}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Закриття зв'язку з клієнтом
admin_client.close()

