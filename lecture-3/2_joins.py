from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType, IntegerType

# Створюємо сесію Spark
spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

# Завантажуємо датасет
nuek_df = spark.read.csv('./data/nuek-vuh3.csv', header=True)

# Виводимо на екран перші 5 записів
nuek_df.show(5)

# Створюємо тимчасове представлення для виконання SQL-запитів
nuek_df.createTempView("nuek_view")

# В яких зонах працюють більш ніж одна пожежна бригада?
# Вибираємо колонки та перейменовуємо 'station_area' на 'station_area_1'
print('Вибираємо колонки та перейменовуємо station_area на station_area_1')
zip_station = nuek_df.select('zipcode_of_incident', 'station_area') \
    .withColumnRenamed("station_area", "station_area_1")
zip_station.show()  # Перевіряємо результат вибору та перейменування

# # Приєднуємо `zip_station` до `nuek_df` на основі колонки `zipcode_of_incident`
# print('Приєднуємо zip_station до nuek_df на основі колонки zipcode_of_incident')
# joined_df = nuek_df.join(zip_station, nuek_df.zipcode_of_incident == zip_station.zipcode_of_incident, 'inner')
# joined_df.show()  # Перевіряємо результат об'єднання
#
# # Видаляємо одну з колонок `zipcode_of_incident`
# print('Видаляємо одну з колонок zipcode_of_incident')
# joined_df = joined_df.drop(zip_station.zipcode_of_incident)
# joined_df.show()  # Перевіряємо результат після видалення колонки
#
# # Вибираємо тільки потрібні колонки
# print('Вибираємо тільки потрібні колонки')
# selected_df = joined_df.select('zipcode_of_incident', 'station_area', 'station_area_1')
# selected_df.show()  # Перевіряємо результат вибору колонок
#
# # Видаляємо дублікатні записи
# print('Видаляємо дублікатні записи')
# dedup_df = selected_df.dropDuplicates(['station_area', 'station_area_1'])
# dedup_df.show()  # Перевіряємо результат видалення дублікатів
#
# # Видаляємо рядки з пустими значеннями
# print('Видаляємо рядки з пустими значеннями')
# non_null_df = dedup_df.dropna()
# non_null_df.show()  # Перевіряємо результат після видалення null значень
#
# # Фільтруємо рядки, де `station_area` та `station_area_1` різні
# print('Фільтруємо рядки, де station_area та station_area_1 різні')
# filtered_df = non_null_df.where(col('station_area') != col('station_area_1'))
# filtered_df.show()  # Перевіряємо результат фільтрації
#
# # Збираємо дані у списки
# print('Згрупуємо за zipcode_of_incident та зберемо списки station_area і station_area_1')
# grouped_df = filtered_df.groupBy('zipcode_of_incident').agg(
#     collect_list("station_area").alias("station_area_list"),
#     collect_list("station_area_1").alias("station_area_list_1")
# )
# grouped_df.show(truncate=False)  # Перевіряємо результат групування
#
# # Об'єднуємо списки `station_area_list` та `station_area_list_1`
# print('Обєднуємо списки station_area_list та station_area_list_1 за допомогою array_union')
# union_df = grouped_df.withColumn("station_area_united", array_union('station_area_list', 'station_area_list_1'))
# union_df.show(truncate=False)  # Перевіряємо результат об'єднання
#
# # Видаляємо дублікатні значення у списку `station_area_united`
# print('Видаляємо унікальні значення у списку station_area_united за допомогою array_distinct')
# distinct_df = union_df.withColumn("station_area_distinct", array_distinct('station_area_united'))
# distinct_df.show(truncate=False)  # Перевіряємо фінальний результат

nuek_df.join(zip_station, nuek_df.zipcode_of_incident == zip_station.zipcode_of_incident, 'inner') \
      .drop(zip_station.zipcode_of_incident) \
      .select('zipcode_of_incident', 'station_area', 'station_area_1') \
      .dropDuplicates(['station_area', 'station_area_1']) \
      .dropna() \
      .where(col('station_area') != col('station_area_1')) \
      .groupBy('zipcode_of_incident') \
      .agg(
          collect_list("station_area").alias("station_area_list"),
          collect_list("station_area_1").alias("station_area_list_1")
          ) \
      .withColumn("station_area_united", array_union('station_area_list', 'station_area_list_1')) \
      .withColumn("station_area_distinct", array_distinct('station_area_united')) \
      .show()
# Закриваємо сесію Spark
spark.stop()