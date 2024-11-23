from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = SparkSession.builder \
    .master("local[3]") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.sql.autoBroadcastJoinThreshold", "0") \
    .appName("MyGoitSparkSandbox") \
    .getOrCreate()

# Завантажуємо датасет
nuek_df = spark.read.csv('C:/Users/user/PycharmProjects/sandbox_pyspark_goit/lecture-3/data/nuek-vuh3.csv', header=True)

nuek_repart = nuek_df.repartition(3)

joined_df = nuek_repart.join(nuek_repart, nuek_df.zipcode_of_incident == nuek_df.zipcode_of_incident, 'inner')

joined_df.count()

input("Press Enter to continue...")

# Закриваємо сесію Spark
spark.stop()