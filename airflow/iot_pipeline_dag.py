from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta


def generate_data():
    print("Simulating: Generate Sensor Data")

def explore_data():
    print("Simulating: Explore Raw Data")

def quality_check():
    print("Simulating: Data Quality Check")

def clean_data():
    print("Simulating: Clean the Data")

def aggregate_daily():
    print("Simulating: Daily Aggregation")

def detect_anomalies():
    print("Simulating: Anomaly Detection")

def visualise():
    print("Simulating: Generate Visualisations")


def make_failure_alert(description):
    def alert(context):
        task_id = context["task_instance"].task_id
        print(f"ALERT: '{task_id}' failed ({description}). Check logs for details.")
    return alert


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

    t1 = PythonOperator(
        task_id="generate_data",
        python_callable=generate_data,
        on_failure_callback=make_failure_alert("Sensor data could not be generated!"),
    )
    t2 = PythonOperator(
        task_id="explore_data",
        python_callable=explore_data,
        on_failure_callback=make_failure_alert("Raw data exploration failed, check the source file exists."),
    )
    t3 = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        on_failure_callback=make_failure_alert("Data quality check failed!"),
    )
    t4 = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        on_failure_callback=make_failure_alert("Data cleaning failed, downstream stages will use stale data."),
    )
    t5 = PythonOperator(
        task_id="aggregate_daily",
        python_callable=aggregate_daily,
        on_failure_callback=make_failure_alert("Daily aggregation failed!"),
    )
    t6 = PythonOperator(
        task_id="detect_anomalies",
        python_callable=detect_anomalies,
        on_failure_callback=make_failure_alert("Anomaly detection failed, no fault alerts will be raised today."),
    )
    t7 = PythonOperator(
        task_id="visualise",
        python_callable=visualise,
        on_failure_callback=make_failure_alert("Chart generation failed!"),
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
