from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder \
    .appName("IoT Data Quality Check") \
    .getOrCreate()

df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

print("=== 1. Missing values per column ===")
df.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df.columns
]).show()

print("=== 2. Duplicate rows ===")
total_rows = df.count()
distinct_rows = df.distinct().count()
print(f"Total rows: {total_rows}")
print(f"Distinct rows: {distinct_rows}")
print(f"Duplicate rows: {total_rows - distinct_rows}")

print("=== 3. Sensor glitch check (VibrationVelocity = -999) ===")
glitch_count = df.filter(col("VibrationVelocity") == -999).count()
print(f"Glitch readings (-999): {glitch_count}")