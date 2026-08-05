from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def generate_data():
    print("Simulating: Generate sensor data")

def explore_data():
    print("Simulating: Explore raw data")

def quality_check():
    print("Simulating: Data quality check")

def clean_data():
    print("Simulating: Clean data")

def aggregate_daily():
    print("Simulating: Daily aggregation")

def detect_anomalies():
    print("Simulating: Anomaly detection")

def visualize():
    print("Simulating: Generate visualisations")

with DAG(
    dag_id="iot_predictive_maintenance_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["iot", "spark", "portfolio"],
) as dag:

    t1 = PythonOperator(task_id="generate_data", python_callable=generate_data)
    t2 = PythonOperator(task_id="explore_data", python_callable=explore_data)
    t3 = PythonOperator(task_id="quality_check", python_callable=quality_check)
    t4 = PythonOperator(task_id="clean_data", python_callable=clean_data)
    t5 = PythonOperator(task_id="aggregate_daily", python_callable=aggregate_daily)
    t6 = PythonOperator(task_id="detect_anomalies", python_callable=detect_anomalies)
    t7 = PythonOperator(task_id="visualize", python_callable=visualize)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
