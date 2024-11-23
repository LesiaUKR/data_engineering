from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Створення SparkSession
spark = SparkSession.builder.appName("CSVStream").getOrCreate()

# Визначення схеми для CSV-файлу
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("value", StringType(), True)
])

# Читання потокових даних із CSV-файлу
csvDF = spark.readStream \
    .option("sep", ",") \
    .option("header", True) \
    .option("maxFilesPerTrigger", 1) \
    .schema(schema) \
    .csv("csv_directory")

# Групування даних за полем 'value' і підрахунок кількості
countDF = csvDF.groupBy("value").count()

# Старт стримінгу та виведення результатів на екран
query = (countDF.writeStream
         .trigger(availableNow=True)
         # .outputMode("complete")
         .outputMode("update")
         .format("console")
         .start()
         )

# Чекаємо кінця роботи стримінгу
query.awaitTermination()