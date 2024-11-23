from pyspark.sql import SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, min, max, avg, unix_timestamp, count_if, round, when
from pyspark.sql.types import TimestampType, IntegerType

# Створюємо сесію Spark
spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

# Завантажуємо датасет
nuek_df = spark.read.csv('./data/nuek-vuh3.csv', header=True)

# Виводимо перші 10 записів
nuek_df.show(10)

# Створюємо тимчасове представлення для виконання SQL-запитів
nuek_df.createTempView("nuek_view")

# Виконуємо SQL-маніпуляції
spark.sql("SELECT * FROM nuek_view LIMIT 15").show()

# Скільки унікальних call_type є в датасеті?
print(nuek_df.select('call_type')
      .where(col("call_type").isNotNull())
      .distinct()
      .count())
# Скільки унікальних call_type є в датасеті? (з використанням SQL)
df = spark.sql("""SELECT COUNT(DISTINCT call_type) as count
                    FROM nuek_view 
                    WHERE call_type IS NOT NULL""")
# Виводимо датафрейм на дисплей
df.show()

# Витягуємо дані колонки з датафрейму
print(df.collect(), type(df.collect()))
# Дотягуємось до самого значення за номером рядка та іменем колонки
print(df.collect()[0]['count'])
# або за номером рядка та номером колонки
print(df.collect()[0][0])

# Які call_type є найбільш популярними (топ-3)?
nuek_df.groupBy('call_type') \
    .count() \
    .orderBy(col('count').desc()) \
    .limit(3) \
    .show()

# Які call_type є найбільш популярними (топ-3)? (з використанням SQL)
spark.sql("""SELECT call_type, COUNT(call_type) as count 
                    FROM nuek_view 
                    GROUP BY call_type 
                    ORDER BY count DESC
                    LIMIT 3""").show()

# різницю між часом, коли диспетчер служби 911 отримав дзвінок (колонка received_dttm) і часом,
# коли команда пожежників виїхала до місця пожежі (колонка response_dttm)

nuek_df.select("received_dttm", "response_dttm") \
    .withColumn("delay_s", col("response_dttm") - (col("received_dttm"))) \
    .show(5)

# результат буде незадовільним, бо обидві колонки мають тип string
nuek_df.select("received_dttm", "response_dttm").printSchema()

# Повторюємо рахування колонок, тільки попередньо
# перетворюємо колонки в тип Timestamp
df_times = nuek_df.select("received_dttm", "response_dttm") \
    .withColumn("received_dttm", col("received_dttm").cast(TimestampType())) \
    .withColumn("response_dttm", col("response_dttm").cast(TimestampType())) \
    .withColumn("delay_s", unix_timestamp(col("response_dttm")) - unix_timestamp(col("received_dttm")))


# Перевіряємо схему нової таблиці
df_times.printSchema()
# Дивимось на результат
df_times.show(5)

df_times.groupby().agg(
    count("*").alias("total_"),
    count_if(col("delay_s").isNotNull()).alias("delayed_not_null"),
    count_if(col("delay_s").isNull()).alias("delayed_null"),
    min(col("delay_s")).alias("min_delay"),
    max(col("delay_s")).alias("max_delay"),
    avg(
        col("delay_s")
    ).alias("avg_delay"),
    avg(
        when(col("delay_s").isNotNull(), col("delay_s")).otherwise(0)
    ).alias("avg_zeroed"),
    avg(
        when(col("delay_s").isNotNull(), col("delay_s")).otherwise(220.0615)
    ).alias("avg_replaced")
).show(10)


# Закриваємо сесію Spark
spark.stop()