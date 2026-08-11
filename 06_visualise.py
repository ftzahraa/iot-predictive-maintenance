from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, when
import matplotlib.pyplot as plt
import seaborn as sns

spark = (
    SparkSession.builder
    .appName("IoT Visualisation")
    .getOrCreate()
)

daily_avg  = spark.read.parquet("daily_averages.parquet")

FAULTY_MACHINES = [3, 11, 17]

pdf = daily_avg.toPandas()

# Chart 1: Vibration Trend Over Time, All Machines
plt.figure(figsize=(12, 6))

for machine_id in sorted(pdf["MachineID"].unique()):
    machine_data = pdf[pdf["MachineID"] == machine_id].sort_values("Date")
    if machine_id in FAULTY_MACHINES:
        plt.plot(machine_data["Date"], machine_data["AvgVibration"],
                 color='red', linewidth=2, label=f"Machine {machine_id} (Faulty)")
    else:
        plt.plot(machine_data["Date"], machine_data["AvgVibration"],
                 color='lightgray', linewidth=0.8)

plt.axhline(y=1.4, color='orange', linestyle='--', linewidth=1, label="Zone A/B Boundary")
plt.axhline(y=2.8, color='darkorange', linestyle='--', linewidth=1, label='Zone B/C Boundary')
plt.axhline(y=4.5, color='red', linestyle='--', linewidth=1, label='Zone C/D Boundary')

plt.title("Vibration Velocity Trend - All 20 Machines Over 30 Days")
plt.xlabel("Date")
plt.ylabel("Vibration Velocity (mm/s RMS)")
plt.xticks(rotation=45)
plt.legend(loc="upper left", fontsize=8)
plt.tight_layout()
plt.savefig("vibration_trend.png", dpi=150)
print("Saved: vibration_trend.png")
plt.close()

# Chart 2: Worst Zone Reached per Machine, Bar Chart
worst = (
    daily_avg.groupBy("MachineID")
    .agg(spark_max("AvgVibration").alias("MaxVibration"))
    .withColumn(
        "Zone",
        when(col("MaxVibration") <= 1.4, "A")
        .when(col("MaxVibration") <= 2.8, "B")
        .when(col("MaxVibration") <= 4.5, "C")
        .otherwise("D")
    )
    .orderBy(col("MaxVibration").desc())
    .toPandas()
)

zone_colors = {"A": "#2ecc71", "B": "#f1c40f", "C": "#e67e22", "D": "#e74c3c"}
bar_colors = worst["Zone"].map(zone_colors)

plt.figure(figsize=(10, 6))
plt.bar(worst["MachineID"].astype(str), worst["MaxVibration"], color=bar_colors)
plt.title("Maximum Vibration Reached per Machine (30-Day Window)")
plt.xlabel("Machine ID")
plt.ylabel("Max Vibration Velocity (mm/s RMS)")
plt.axhline(y=1.4, color='gray', linestyle='--', linewidth=0.8)
plt.axhline(y=2.8, color='gray', linestyle='--', linewidth=0.8)
plt.axhline(y=4.5, color='gray', linestyle='--', linewidth=0.8)
plt.tight_layout()
plt.savefig("max_vibration_by_machine.png", dpi=150)
print("Saved: max_vibration_by_machine.png")
plt.close()

# Chart 3: Z-score Trend Over Time, Per-machine Baseline Deviation
scored = spark.read.parquet("statistical_analysis.parquet")
scored_pdf = scored.toPandas()

plt.figure(figsize=(12, 6))

for machine_id in sorted(scored_pdf["MachineID"].unique()):
    machine_data = scored_pdf[scored_pdf["MachineID"] == machine_id].sort_values("Date")
    if machine_id in FAULTY_MACHINES:
        plt.plot(machine_data["Date"], machine_data["ZScore"],
                 color='red', linewidth=2, label=f"Machine {machine_id} (Faulty)")
    else:
        plt.plot(machine_data["Date"], machine_data["ZScore"],
                 color='lightgray', linewidth=0.8)

plt.axhline(y=3, color='blue', linestyle='--', linewidth=1, label="Z-score = 3 (Statistical Threshold)")
plt.axhline(y=-3, color='blue', linestyle='--', linewidth=1)

plt.title("Per-Machine Baseline Deviation (Z-score) Over 30 Days")
plt.xlabel("Date")
plt.ylabel("Z-score (Standard Deviations From Own Baseline)")
plt.xticks(rotation=45)
plt.legend(loc="upper left", fontsize=8)
plt.tight_layout()
plt.savefig("zscore_trend.png", dpi=150)
print("Saved: zscore_trend.png")
plt.close()

spark.stop()
