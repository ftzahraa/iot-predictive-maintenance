from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, max as spark_max

spark = (
    SparkSession.builder
    .appName("IoT Anomaly Detection")
    .getOrCreate()
)

daily_avg = spark.read.parquet("daily_averages.parquet")

# Flag Each Day's Reading Against Real ISO 20816-3 / Bearing Temp Thresholds
flagged = daily_avg.withColumn(
    "VibrationZone",
    when(col("AvgVibration") <= 1.4, "A")
    .when(col("AvgVibration") <= 2.8, "B")
    .when(col("AvgVibration") <= 4.5, "C")
    .otherwise("D")
).withColumn(
    "TemperatureAlert",
    when(col("AvgTemperature") >= 90, "Trip")
    .when(col("AvgTemperature") >= 80, "Alarm")
    .otherwise("Normal")
)

print("- Days Where Any Machine Reached Zone C or D (Restricted/Danger) -")
flagged.filter(col("VibrationZone").isin("C", "D")) \
    .orderBy("MachineID", "Date") \
    .show(50)

print("- Summary: Worst Vibration Zone Reached per Machine, Over the Full 30 Days -")
worst_per_machine = daily_avg.groupBy("MachineID") \
    .agg(spark_max("AvgVibration").alias("MaxVibrationReached"),
         spark_max("AvgTemperature").alias("MaxTemperatureReached")) \
    .withColumn(
        "WorstZoneReached",
        when(col("MaxVibrationReached") <= 1.4, "A")
        .when(col("MaxVibrationReached") <= 2.8, "B")
        .when(col("MaxVibrationReached") <= 4.5, "C")
        .otherwise("D")
    ) \
    .orderBy(col("MaxVibrationReached").desc())

worst_per_machine.show(20)

flagged.write.mode("overwrite").parquet("flagged_data.parquet")
print("Flagged Data Saved to flagged_data.parquet")

spark.stop()
