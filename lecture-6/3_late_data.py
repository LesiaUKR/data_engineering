from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import window

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
    .csv("csv_directory_late") \
    .withWatermark("event_time", "20 seconds")

# Групування даних за полем 'value' і підрахунок кількості
countDF = (csvDF
           .groupBy(
    window("event_time", "1 minutes"),
    "value"
).count())

# Старт стримінгу й виведення результатів на екран
query = (countDF.writeStream
         .trigger(processingTime='10 seconds')
         # .outputMode("complete")
         # .outputMode("update")
         .outputMode("append")
         .format("console")
         .options(
    truncate=False,
    numRows=20,
)
         .start()
         )

# Чекаємо кінця роботи стримінгу
query.awaitTermination()