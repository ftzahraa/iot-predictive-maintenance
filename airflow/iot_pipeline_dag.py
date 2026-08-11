from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from datetime import datetime, timedelta


def make_failure_alert(description):
    def alert(context):
        task_id = context["task_instance"].task_id
        print(f"ALERT: '{task_id}' failed ({description}). Check logs for details.")
    return alert


SCRIPTS_DIR = "/opt/airflow/scripts"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="iot_predictive_maintenance_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["iot", "spark", "portfolio"],
) as dag:

    t1 = BashOperator(
        task_id="generate_data",
        bash_command=f"cd {SCRIPTS_DIR} && python generate_data.py",
        on_failure_callback=make_failure_alert("sensor data could not be generated"),
    )
    t2 = BashOperator(
        task_id="explore_data",
        bash_command=f"cd {SCRIPTS_DIR} && python 01_explore_data.py",
        on_failure_callback=make_failure_alert("raw data exploration failed, check the source file exists"),
    )
    t3 = BashOperator(
        task_id="quality_check",
        bash_command=f"cd {SCRIPTS_DIR} && python 02_data_quality_check.py",
        on_failure_callback=make_failure_alert("data quality check failed"),
    )
    t4 = BashOperator(
        task_id="clean_data",
        bash_command=f"cd {SCRIPTS_DIR} && python 03_clean_data.py",
        on_failure_callback=make_failure_alert("data cleaning failed, downstream stages will use stale data"),
    )
    t5 = BashOperator(
        task_id="aggregate_daily",
        bash_command=f"cd {SCRIPTS_DIR} && python 04_analysis.py",
        on_failure_callback=make_failure_alert("daily aggregation failed"),
    )
    t6 = BashOperator(
        task_id="detect_anomalies",
        bash_command=f"cd {SCRIPTS_DIR} && python 05_anomaly_detection.py",
        on_failure_callback=make_failure_alert("anomaly detection failed, no fault alerts will be raised today"),
    )
    t7 = BashOperator(
        task_id="statistical_analysis",
        bash_command=f"cd {SCRIPTS_DIR} && python 08_statistical_analysis.py",
        on_failure_callback=make_failure_alert("statistical baseline deviation analysis failed"),
    )
    t8 = BashOperator(
        task_id="visualise",
        bash_command=f"cd {SCRIPTS_DIR} && python 06_visualise.py",
        on_failure_callback=make_failure_alert("chart generation failed"),
    )
    t9 = BashOperator(
        task_id="upload_to_cloud",
        bash_command=f"cd {SCRIPTS_DIR} && python 07_upload_to_cloud.py",
        on_failure_callback=make_failure_alert("cloud upload failed"),
    )
    t10 = WasbBlobSensor(
        task_id="verify_cloud_upload",
        container_name="iot-pipeline-data",
        blob_name="flagged_data.parquet/_SUCCESS",
        wasb_conn_id="azure_blob_default",
        timeout=60,
        poke_interval=10,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9 >> t10
