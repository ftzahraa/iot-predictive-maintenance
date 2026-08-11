from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, mean as spark_mean, stddev as spark_stddev, abs as spark_abs, max as spark_max

spark = (
    SparkSession.builder
    .appName("IoT Statistical Analysis")
    .getOrCreate()
)

daily_avg = spark.read.parquet("daily_averages.parquet")

# Per-machine baseline, not a shared fleet-wide one.
# Each machine's own first 7 days establish what "normal" looks like for
# that specific machine, mirroring how real condition monitoring systems
# track baseline deviation per asset, not against a fleet-wide average.
BASELINE_DAYS = 7

baseline_window = daily_avg.filter(col("Date") <= "2026-01-07")

baseline_per_machine = baseline_window.groupBy("MachineID").agg(
    spark_mean("AvgVibration").alias("BaselineMean"),
    spark_stddev("AvgVibration").alias("BaselineStdDev")
)

print("=== Per-machine healthy baseline (first 7 days) ===")
baseline_per_machine.orderBy("MachineID").show(20)

# Join each day's reading back to its own machine's baseline
scored = daily_avg.join(baseline_per_machine, on="MachineID", how="left")

scored = scored.withColumn(
    "ZScore",
    (col("AvgVibration") - col("BaselineMean")) / col("BaselineStdDev")
)

print("\n=== Days where any machine's Z-score exceeds 3, against its own baseline ===")
scored.filter(spark_abs(col("ZScore")) > 3).orderBy("MachineID", "Date").show(50)

print("\n=== Worst (highest) Z-score reached per machine, against its own baseline ===")
worst_z = scored.groupBy("MachineID").agg(
    spark_max("ZScore").alias("MaxZScore")
).orderBy(col("MaxZScore").desc())

worst_z.show(20)

scored.write.mode("overwrite").parquet("statistical_analysis.parquet")
print("\nStatistical analysis saved to statistical_analysis.parquet")

spark.stop()