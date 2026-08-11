from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("IoT Data Cleaning") \
    .getOrCreate()

df = spark.read.csv("sensor_data.csv", header =  True, inferSchema = True)

original_count = df.count()

# Step 1: Remove Exact Duplicate Rows
df = df.dropDuplicates()
after_dedup_count = df.count()

# Step 2: Treat -999 (impossible reading) as invalid -> convert to null
df = df.withColumn("VibrationVelocity", when (col("VibrationVelocity") == -999, None).otherwise(col("VibrationVelocity")))

# Step 3: Drop Rows with Any Missing Sensor Reading
sensor_columns = ["VibrationVelocity", "BearingTemperature", "MotorCurrentDraw", "RotationalSpeed"]
df_clean = df.dropna(subset=sensor_columns)
final_count = df_clean.count()

# Report What Happened at Each Step
print(f"Original rows: {original_count}")
print(f"After removing duplicates: {after_dedup_count} (removepythond {original_count - after_dedup_count})")
print(f"After removing rows with missing/invalid sensor data: {final_count} (removed {after_dedup_count - final_count})")
print(f"Total rows removed: {original_count - final_count} ({round((original_count - final_count) / original_count * 100, 2)}%)")

# Save the Cleaned Data as Parquet (a Proper Format for the Next Stage, not CSV)
df_clean.write.mode("overwrite").parquet("sensor_data_clean.parquet")
print("Cleaned Data Saved to sensor_data_clean.parquet")

spark.stop()
