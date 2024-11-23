import random
from pyspark.sql import SparkSession

# Ініціалізація SparkSession і SparkContext
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("PiCalculation") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
# Функція для перевірки, чи точка знаходиться всередині кола
def inside(p):
    x, y = random.random(), random.random()
    return x * x + y * y < 1

# Невеликий обсяг даних для тесту
num_samples = 100000
count = sc.parallelize(range(0, num_samples)).filter(inside).count()
print("Зразок тестового обчислення Pi з малим обсягом: %f" % (4.0 * count / num_samples))

# Завершення SparkSession
spark.stop()







