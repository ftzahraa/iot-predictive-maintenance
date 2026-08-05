from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Inspect Results").getOrCreate()

flagged = spark.read.parquet("flagged_data.parquet")

print("- All Flagged Anomalies (Zone C or D) -")
flagged.filter(col("VibrationZone").isin("C", "D")).orderBy("MachineID", "Date").show(100)

print(f"\nTotal Anomaly-Days Recorded: {flagged.filter(col('VibrationZone').isin('C', 'D')).count()}")
print(f"Machines Ever Reaching Zone D (Danger): {[row.MachineID for row in flagged.filter(col('VibrationZone') == 'D').select('MachineID').distinct().collect()]}")

spark.stop()
