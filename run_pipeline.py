import subprocess
import sys
from datetime import datetime

# The Pipeline Stages, in Order
STAGES = [
    ("Generate Simulated Sensor Data", "generate_data.py"),
    ("Explore Raw Data", "01_explore_data.py"),
    ("Data Quality Check", "02_data_quality_check.py"),
    ("Clean the Data", "03_clean_data.py"),
    ("Daily Aggregation Analysis", "04_analysis.py"),
    ("Anomaly Detection", "05_anomaly_detection.py"),
    ("Statistical baseline deviation analysis", "08_statistical_analysis.py"),
    ("Generate Visualisations", "06_visualise.py"),
]

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def run_pipeline():
    log("- IoT Sensor Pipeline Started -")

    for stage_number, (description, script_name) in enumerate(STAGES, start=1):
        log(f"- Stage {stage_number}/{len(STAGES)}: {description} ({script_name}) -")

        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            log(f"FAILED at Stage {stage_number}: {description}")
            log("- Error Output -")
            print(result.stderr)
            log("- Pipeline Stopped Due to Error -")
            sys.exit(1) # Stope the whole pipeline, don't continue to broken next steps

        log(f"Stage {stage_number} Completed Successfully!")

    log("- Pipeline Completed Successfully, All Stgaes Passed -")

if __name__ == "__main__":
    run_pipeline()
