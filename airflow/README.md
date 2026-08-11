# Airflow Orchestration

Part of the [IoT Predictive Maintenance Pipeline](../README.md) project. A
working Apache Airflow DAG that runs the real IoT pipeline end to end,
inside Docker. This is no longer a placeholder demonstration alongside the
lightweight Python orchestrator in the repo root, it genuinely runs the same
PySpark scripts, pushes results to Azure, and verifies the upload, all
through the tool that keeps showing up in real data engineering job
postings.

## Why this exists
The lightweight orchestrator proves the automation concept works. This DAG
proves the same thing with the tool teams actually use in production: real
task dependencies, scheduling, retries, per-task alerting, a visual DAG
graph, and real execution of the actual pipeline against real cloud storage,
not just a demonstration of the shape of it.

## What it does
The Airflow image is built from a custom `Dockerfile` on top of
`apache/airflow:3.3.0`, with Java 17 and PySpark 4.2.0 installed directly
inside it, alongside `apache-airflow-providers-microsoft-azure`. The actual
pipeline scripts live in the `scripts/` folder here, and each of the ten
tasks in the DAG runs one of them as a real command through `BashOperator`,
in order: generate the sensor data, explore it, check its quality, clean it,
aggregate it daily, detect anomalies against the ISO 20816-3 thresholds, run
a per-machine statistical baseline check, build the charts, push the results
to Azure Blob Storage, then finish with a `WasbBlobSensor` that checks Azure
directly for the `_SUCCESS` marker to confirm the upload genuinely landed.

The statistical task compares each machine's daily vibration against its
own first-week baseline rather than a shared fleet average, flagging any
reading more than three standard deviations from that machine's own normal.
It runs alongside the threshold-based anomaly detection rather than
replacing it, the two methods catch overlapping but not identical things,
and comparing them against each other is part of the point.

The scripts in this folder are copies of the root-level pipeline scripts,
duplicated because Docker can only mount files that live inside this
specific `airflow` folder, not the separate local project folder they were
originally built in. Same code, two places, for a genuine technical reason
rather than an oversight.

Early versions of this DAG used placeholder functions instead, since
bridging Airflow's containers back to PySpark running on the Windows host
would have meant SSHing back to a laptop, which isn't representative of a
real setup. Running PySpark directly inside the Linux-based Airflow
container turned out to be the cleaner answer, and it sidesteps the whole
Windows/Hadoop/winutils headache from the local setup entirely, since none
of that is a Windows-specific problem on Linux.

The DAG runs on a daily schedule (`schedule="@daily"`), retries a failed
task twice with a two-minute delay between attempts, and gives each task its
own specific failure alert rather than one generic message shared across
all of them.

## Running it
```
docker compose build
docker compose up airflow-init
docker compose up
```
Then open `http://localhost:8080` and log in with `airflow` / `airflow`. The
DAG shows up as `iot_predictive_maintenance_pipeline` and can be triggered
manually from the UI, or left alone to run on its daily schedule.

An Airflow connection needs to exist for the Azure steps to work:
**Admin → Connections**, Connection Id `azure_blob_default`, type
"Azure Blob Storage," with the storage account name and access key filled
into the Login and Blob Storage Key fields. The connection string used by
the upload script itself is passed in as an environment variable through
`.env`, never hardcoded anywhere in the code.

This also needs the `docker-compose.yaml` file from Apache's official
Airflow Quick Start guide, along with `dags`, `logs`, `plugins`, and
`scripts` folders and a `.env` file setting `AIRFLOW_UID=50000`. The
generated `dags`, `logs`, and `plugins` folders aren't included here since
they're either large, auto-generated, or standard boilerplate rather than
project-specific code.

## A verified run
All ten tasks completed successfully on a manual trigger, genuinely
executing the real pipeline: fresh sensor data generated, cleaned, checked
against both the fixed thresholds and each machine's own statistical
baseline, visualised, uploaded to Azure, and the upload independently
confirmed by the sensor task checking Azure directly. This run is noticeably
slower than the earlier placeholder version, since it's now spinning up a
real Spark session for each stage rather than printing a line, which is
expected and correct.

## Getting this working wasn't smooth, and that's worth documenting
Getting Airflow itself running on Windows meant installing Docker Desktop,
which needed WSL2, which wasn't installed yet, so that meant a `wsl
--install` and a restart before Docker would even start.

Adding real PySpark execution meant rebuilding the Airflow image with Java
and PySpark installed directly inside it, a `Dockerfile` and
`requirements.txt`, a `build: .` line in `docker-compose.yaml`, and a
`scripts` volume mount to make the actual pipeline files visible inside the
container. The rebuild itself, installing a JDK and a 450MB PySpark package
inside the image, took a couple of minutes but completed cleanly on the
first attempt, genuinely smoother than the equivalent Windows setup had
been, since Linux doesn't need winutils or a manually hunted-down
`JAVA_HOME` path.

## Next steps
- Extend the Azure verification step to inspect the actual anomaly counts in the uploaded data, not just confirm the file exists
- Add a Slack or email notifier in place of the current print-based alert
