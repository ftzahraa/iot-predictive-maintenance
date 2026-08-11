from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs as spark_abs

spark = SparkSession.builder.appName("Inspect Results").getOrCreate()

flagged = spark.read.parquet("flagged_data.parquet")

print("- All Flagged Anomalies (Zone C or D) -")
flagged.filter(col("VibrationZone").isin("C", "D")).orderBy("MachineID", "Date").show(100)

print(f"\nTotal Anomaly-Days Recorded: {flagged.filter(col('VibrationZone').isin('C', 'D')).count()}")
print(f"Machines Ever Reaching Zone D (Danger): {[row.MachineID for row in flagged.filter(col('VibrationZone') == 'D').select('MachineID').distinct().collect()]}")

print("\n\n- Statistical Baseline Deviation Results -")
scored = spark.read.parquet("statistical_analysis.parquet")

print("- All Days With a Statistically Significant Deviation (|Z-score| > 3) -")
scored.filter(spark_abs(col("ZScore")) > 3).orderBy("MachineID", "Date").show(100)

print(f"\nTotal Statistically Significant Deviation-Days: {scored.filter(spark_abs(col('ZScore')) > 3).count()}")
print(f"Machines With a Z-score Ever Exceeding 3: {[row.MachineID for row in scored.filter(spark_abs(col('ZScore')) > 3).select('MachineID').distinct().collect()]}")

spark.stop()
