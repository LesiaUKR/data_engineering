from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, concat_ws, to_json, struct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os

# Задаємо ім'я топіка
my_name = "oleksiy"
topic_name_in = f'{my_name}_spark_streaming_in'
topic_name_out = f'{my_name}_spark_streaming_out'

# Пакети для роботи з Kafka
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

# Читання потоку даних із Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("subscribe", topic_name_in) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .load()

# Схема JSON
json_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("value", IntegerType(), True)
])

# Обробка даних
clean_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .withColumn("value_json", from_json(col("value"), json_schema)) \
    .withColumn("timestamp", col("value_json.timestamp")) \
    .withColumn("value", col("value_json.value")) \
    .withColumn("new_value", concat_ws("-", lit("I_love_spark"), col("value")))

# Підготовка даних для запису в Kafka (перетворюємо value у string через to_json)
output_df = clean_df.select(
    col("key").cast("STRING").alias("key"),  # Перетворення ключа у STRING
    to_json(struct(
        col("value"),  # Оригінальне значення
        col("new_value"),  # Додаткове поле
        col("timestamp")  # Часова мітка
    )).alias("value")  # Перетворення значення у JSON STRING
)

# Виведення даних на консоль
clean_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Запис оброблених даних у Kafka
output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", topic_name_out) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start() \
    .awaitTermination()
