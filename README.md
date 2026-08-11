# IoT Predictive Maintenance Pipeline

A PySpark pipeline that simulates industrial sensor data from rotating machinery
(vibration, temperature, current draw) and automatically flags developing
equipment faults. The detection thresholds aren't guesses; they come straight
from ISO 20816-3, the real vibration standard used in industry.

## Why this project
Ireland has a strong industrial IoT base, and vibration/temperature monitoring
on rotating machinery is one of the most common real predictive maintenance
setups in use today. Companies like Lumina run this kind of monitoring at
Irish manufacturing sites such as Mergon Group. This project builds a
simplified but genuinely standards-grounded version of that same idea.

## What it does
Thirty days of sensor readings get simulated across 20 machines, with the
usual mess real sensors produce baked in: missing readings, duplicate
transmissions, the odd glitchy value. Three of the machines are seeded with a
slow-developing bearing fault, so their vibration and temperature climb
together over time, the way an actual failing bearing behaves. A PySpark
pipeline then ingests all of it, cleans it up, aggregates it daily, and flags
anything that crosses into real ISO 20816-3 danger territory. Alongside that,
a second detection method checks each machine against its own individual
baseline, catching drift a fixed threshold alone would miss.

The whole thing runs end to end from one command, and the same pipeline has
also been rebuilt as a real Apache Airflow DAG (see [`airflow/`](./airflow/)),
running the actual PySpark scripts inside Docker, not just a placeholder
demonstration, and pushing the results to Azure Blob Storage in Ireland's own
data centre region, with a final task that independently verifies the upload
landed.

## Pipeline stages
1. **Generate data**: simulate the sensor readings, mess included
2. **Explore data**: check the schema, look at a sample
3. **Quality check**: count up the nulls, duplicates, bad readings
4. **Clean data**: drop duplicates, null out invalid readings, remove incomplete rows, save as Parquet
5. **Aggregate daily**: collapse everything into daily per-machine averages
6. **Detect anomalies**: flag readings against ISO 20816-3 zones and bearing temperature thresholds
7. **Statistical analysis**: compare each machine against its own baseline using Z-scores, catching deviation a fixed threshold alone would miss
8. **Visualise**: build trend and summary charts
9. **Upload to cloud**: push the flagged results to Azure Blob Storage
10. **Verify**: confirm the upload genuinely landed (Airflow version only)

## What it found
The three machines seeded with a fault (3, 11, and 17) were exactly the ones
the pipeline flagged, with no false positives and no ambiguity, in the
threshold-based check. All three climbed from Zone A (good) into Zone D
(danger) over the 30 days, and their bearing temperatures approached the
95°C industry ceiling right around the same time. The two failure indicators
rose together, which is genuinely how real bearing degradation looks. The
other 17 machines stayed flat and healthy the whole time. 51 anomaly-days got
flagged in total, all of them from the same three machines.

The statistical layer told a related but more nuanced story. Comparing each
machine's daily vibration against its own first-week baseline, the three
faulty machines again stood out clearly, their Z-scores climbed steadily
past 10, far beyond the statistical significance threshold of 3, while
staying there for the rest of the window. But six of the healthy machines
also crossed that threshold occasionally, on isolated single days, purely
from natural random noise. That's not a flaw in the method, it's exactly why
real condition monitoring systems, including Lumina's, run several detection
methods together rather than relying on one. A single statistical check
alone will produce some false positives on noisy data; corroboration across
methods is what makes a flag trustworthy.

## Tools
- PySpark 4.2.0
- Python for the data simulation and orchestration
- Matplotlib / Seaborn for the charts
- Parquet between pipeline stages
- Apache Airflow (via Docker), running the real pipeline end to end, not a placeholder version
- Azure Blob Storage (North Europe region) for cloud output storage, verified directly from within the Airflow DAG

## How to run

**Locally:**
```
pip install pyspark matplotlib seaborn pandas pyarrow azure-storage-blob
python run_pipeline.py
```
To push the flagged results to Azure Blob Storage, set an `AZURE_STORAGE_CONNECTION_STRING` environment variable (never hardcoded in the script itself) and run:
```
python 07_upload_to_cloud.py
```
To check the results afterwards without re-running everything:
```
python inspect_results.py
```

**Via Airflow**, the full pipeline runs inside Docker, including the cloud
upload and verification, see [`airflow/README.md`](./airflow/README.md) for
the complete setup.

**A note for Windows users:** running PySpark directly on Windows (outside
Airflow's Linux containers) needs Java 17 or 21, plus a Hadoop
`winutils.exe` and `hadoop.dll` for file writing, with `JAVA_HOME` and
`HADOOP_HOME` both set. This caused most of the real trouble during local
setup, and is one of the reasons the Airflow version runs PySpark inside its
Linux container instead, avoiding the issue entirely.

## The thresholds, and where they came from
- **Vibration (ISO 20816-3, Group 2 machines):** Zone A up to 1.4 mm/s is good, Zone B up to 2.8 is acceptable, Zone C up to 4.5 means something's developing, and anything past that is Zone D, genuine danger.
- **Bearing temperature:** 80°C is typically the alarm point, 90°C the trip point, and rolling bearings generally shouldn't exceed 95°C.
- **Statistical significance:** a Z-score beyond ±3 standard deviations from a machine's own baseline, the standard three-sigma threshold, calculated per machine from its own first seven days rather than against a shared fleet-wide average.

## Getting this working wasn't smooth, and that's worth documenting
Spark's `.parquet()` writes kept failing on Windows with a
`HADOOP_HOME and hadoop.home.dir are unset` error. Adding `winutils.exe` and
`hadoop.dll` got further but then hit an `UnsatisfiedLinkError`. It turned out
the DLL had downloaded as a corrupted, incomplete `.crdownload` file after
Chrome blocked it as a security risk. Re-downloading it through PowerShell's
`Invoke-WebRequest` instead of the browser fixed it properly.

Automation broke in a more interesting way: an `input()` prompt added during
development, just to pause and check the Spark UI, silently hung the
automated pipeline for 7 minutes on a stage that normally finishes in 11
seconds, since nothing was there to press Enter.

Setting up Airflow meant installing Docker Desktop, which needed WSL2, which
wasn't installed yet, so that meant a `wsl --install` and a restart before
Docker would even start. Getting Airflow to run the real PySpark scripts,
rather than placeholders, meant rebuilding its image with Java and PySpark
installed directly inside the container, which turned out to be genuinely
smoother than the Windows setup had been, since Linux doesn't need winutils
or a manually configured `JAVA_HOME`.

The Azure storage account name had to be tried twice, since account names
are globally unique across every Azure customer and the first choice was
already taken elsewhere.

The first version of the statistical baseline compared every machine against
one shared, fleet-wide average, rather than each machine's own individual
history. That's a genuinely different, less accurate method than what real
condition monitoring systems use, and it was corrected to a per-machine
baseline once the distinction became clear, each machine's own first seven
days now establishes what normal looks like for that specific machine.

## Data quality, checked against what was actually injected
The messiness built into the data landed almost exactly where it was supposed
to: about 2% missing values (2.0% actual), about 1% duplicates (0.98% actual),
about 0.5% sensor glitches (0.49% actual). That match is itself a useful
sanity check; it confirms the cleaning pipeline is genuinely catching what it
should, not just running without errors.

## A note on how realistic this actually is
This simulation simplifies real sensor behaviour on purpose, for the sake of
clarity. Real degrading bearings are noisier and less linear than the smooth
climb modelled here, and real deployments deal with things this version
doesn't touch: clock drift, irregular reporting intervals, more than one kind
of fault happening at once. The thresholds and the underlying failure physics
are real and standards-based; the data generation is a reasonable
simplification of them, not a claim that this is exactly what a factory floor
looks like.

## Next steps
- Add a machine learning layer to predict failure ahead of a fixed threshold being crossed, shaped around the same detection concepts real systems use: rate of change, per-machine anomaly detection, and a continuous risk score rather than a simple flagged/not-flagged outcome
- Build a version with messier, less idealised sensor behaviour
