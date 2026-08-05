# Airflow Orchestration

A working Apache Airflow DAG for the same IoT pipeline, run locally through Docker. It sits alongside the lightweight Python orchestrator in the repo root as a second way of running the same seven stages, this time using the tool that commonly shows up in real data engineering job postings.

## Why this exists
The lightweight orchestrator already proves the automation concept works. Airflow proves the same thing with the tool teams actually use in production: real task dependencies, a visual DAG graph, retry handling, and a proper web interface for watching runs, rather than just reading log lines scroll past in a terminal.

## What it does right now
The DAG defines the same seven stages as the main pipeline, in the same order, as separate Airflow tasks with explicit dependencies between them (`t1 >> t2 >> t3 ...`). Each task currently runs a placeholder function instead of calling the actual PySpark scripts. That's deliberate. Getting the orchestration and dependency structure working cleanly on its own came first, before tackling the separate problem of connecting Airflow's Docker containers to the PySpark environment running outside them.

## Running it
```
docker compose up airflow-init
docker compose up
```
Then open `http://localhost:8080` and log in with `airflow` / `airflow`. The DAG shows up as `iot_predictive_maintenance_pipeline` and can be triggered manually from the UI.

This needs the `docker-compose.yaml` file from Apache's official Airflow Quick Start guide, along with `dags`, `logs`, and `plugins` folders and a `.env` file setting `AIRFLOW_UID=50000`. None of that is included here since it's either large, auto-generated, or standard boilerplate rather than project-specific code.

## A verified run
All seven tasks completed successfully on a manual trigger, finishing in about 10 seconds: `generate_data`, then `explore_data`, `quality_check`, `clean_data`, `aggregate_daily`, `detect_anomalies`, and finally `visualise`. Each one logged individually with its own duration and try count in the Airflow UI.

## Getting Airflow running on Windows wasn't straightforward either
Installing Airflow directly with pip is only officially supported on Linux, and it's known to be unreliable on native Windows. Docker is the standard way around that, which meant installing Docker Desktop first. Docker Desktop in turn needed WSL2, which wasn't installed yet, so that meant a separate `wsl --install` and a restart before Docker would even start. Once that was sorted, the rest of the setup, the `.env` file, the folder structure, initialising the database, starting the services, followed Apache's own Quick Start guide fairly directly.

## Next steps
- Replace the placeholder task functions with real calls into the PySpark scripts, once there's a clean way to bridge Airflow's containers and the local Python environment
- Add a proper schedule instead of manual-only triggering
- Add retry and alerting configuration to the tasks
