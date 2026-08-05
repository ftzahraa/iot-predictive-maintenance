from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, round as spark_round

spark = SparkSession.builder \
    .appName("IoT Analysis") \
    .getOrCreate()

df = spark.read.parquet("sensor_data_clean.parquet")

# Add a "Data" Column (Just the Day, Dropping the Time) for Daily Grouping
df = df.withColumn("Date", to_date(col("Timestamp")))

# Daily Average Vibration and Temperature, per Machine
daily_avg = df.groupBy("MachineID", "Date") \
    .agg(spark_round(avg("VibrationVelocity"), 3).alias("AvgVibration"),
         spark_round(avg("BearingTemperature"), 2).alias("AvgTemperature")
    ) \
    .orderBy("MachineID", "Date")

print("- Daily Averages for Machine 11 (a known faulty machine) -")
daily_avg.filter(col("MachineID") == 11).show(30)

print("- Daily Averages for Machine 1 (a known healthy machine) -")
daily_avg.filter(col("MachineID") == 1).show(30)

# Save This for Later Use (Dashboarding Stage)
daily_avg.write.mode("overwrite").parquet("daily_averages.parquet")
print("Daily Averages Saved to daily_averages.parquet")

spark.stop()
         