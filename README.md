# Data Engineering Final Project

This repository contains the GoIT Data Engineering final project implementation. It uses PySpark for the batch data lake and includes a MySQL reader for the streaming task.

## Implementation

- `read_mysql.py` reads `athlete_bio` and `athlete_event_results` from MySQL. It casts `height` and `weight` to numbers, filters invalid values from `athlete_bio`, and prints sample rows.
- `mysql_to_kafka.py` reads `athlete_event_results` from MySQL and writes every row as a JSON message to `KAFKA_INPUT_TOPIC` (key: `athlete_id`). It uses the `KAFKA_*` SASL settings from `.env`.
- `streaming_pipeline.py` reads `athlete_bio` from MySQL and filters invalid height and weight (non-numeric values and weights outside 20–250 kg), streams results from `KAFKA_INPUT_TOPIC`, parses the JSON into columns, joins both on `athlete_id`, and calculates average height and weight by sport, medal, sex and country with a calculation timestamp. Every micro-batch is written with `foreachBatch` to `KAFKA_OUTPUT_TOPIC` and appended to the MySQL table `OUTPUT_MYSQL_DATABASE.OUTPUT_MYSQL_TABLE` (defaults: `neo_data.athlete_enriched_agg`). The query keeps running until you stop it with Ctrl+C; the checkpoint is stored in `data/checkpoints/streaming_pipeline/`.
- `landing_to_bronze.py` downloads `athlete_bio.csv` and `athlete_event_results.csv` from `SOURCE_URL`, saves the original CSV files under `data/landing/`, then writes Parquet datasets under `data/bronze/`.
- `bronze_to_silver.py` cleans text columns, removes duplicate rows, and writes Parquet datasets under `data/silver/`.
- `silver_to_gold.py` joins the Silver datasets on `athlete_id`, filters invalid height and weight values, calculates average height and weight by sport, medal, sex and country, adds a timestamp, and writes `data/gold/avg_stats/`.
- `project_solution.py` defines the `athlete_batch_data_lake` Airflow DAG. It runs the three Spark jobs in order with `SparkSubmitOperator`.
- `run.sh` builds the Docker image and runs one of the supported scripts.
- `Dockerfile` installs Java 17, the Python dependencies (PySpark 4.3.0), MySQL Connector/J 8.0.32, and the Spark Kafka connector jars. Their paths are in the `MYSQL_JDBC_JAR` and `KAFKA_JARS` environment variables.

## Requirements

- Docker
- Internet access to build the image and download source CSV files
- MySQL credentials for `read_mysql.py` and `print_json`
- Kafka broker address and PLAIN SASL credentials in `.kcat-goit.conf` for `create_topics`

## Configuration

Copy `.env.example` to `.env` and fill in the required values:

```env
MYSQL_HOST=your-mysql-host
MYSQL_PORT=3306
MYSQL_DATABASE=olympic_dataset
MYSQL_USER=your-mysql-user
MYSQL_PASSWORD=your-mysql-password
SOURCE_URL=your-csv-source-base-url
```

Keep `.env` private. It is excluded from Git by `.gitignore`.

`SOURCE_URL` is the base URL for the CSV files. The script appends the CSV filenames to this value. It must be set to run `landing_to_bronze.py`.

## Run a script

Run commands from the repository directory (`goit-de-fp`):

```sh
./run.sh landing_to_bronze
./run.sh bronze_to_silver
./run.sh silver_to_gold
./run.sh read_mysql
./run.sh mysql_to_kafka
./run.sh streaming_pipeline
./run.sh print_json
```

`run.sh` builds or reuses the `spark-mysql-driver:latest` image, then runs the selected script in a temporary container. For every Python script except `read_mysql`, the local `data/` directory is mounted into the container as `/app/data`, so CSV and Parquet output persist on your machine. All scripts receive the values from `.env`.

`print_json` uses the MySQL settings in `.env` to print a single `athlete_event_results` record as JSON. For Python scripts in the repository root, pass the filename with or without `.py`.

The complete batch pipeline can be run locally in sequence with the first three commands above. The Airflow DAG expects the three Spark job files to be available in the Airflow DAG folder, an Airflow Spark connection named `spark-default`, and a `DATA_DIR` path visible to the Spark driver. Set `SPARK_CONN_ID` or `DATA_DIR` in the Airflow environment to override the defaults. Set `SOURCE_URL` in the Airflow task environment so the landing job can download the source CSV files.

## Create Kafka topics

The command looks for `kafka/bin/kafka-topics.sh` and then searches `PATH`.
If neither has the CLI, Docker runs it from the official `apache/kafka:4.1.2`
image. In that case Docker provides Java and downloads the image on first use.

Set `KAFKA_INPUT_TOPIC` and `KAFKA_OUTPUT_TOPIC` in `.env` using your topic
names from the shared cluster. Use unquoted `KEY=value` lines.

From the repository directory, run:

```sh
./run.sh create_topics
```

The command uses PLAIN SASL credentials from `.kcat-goit.conf` and deletes
the temporary Java client configuration when it exits. It uses the broker
address from `.kcat-goit.conf` unless `KAFKA_BOOTSTRAP_SERVERS` is set in
`.env`. Existing topics are skipped.

## Data layout

After a successful batch pipeline run, the data directory will look like this:

```text
data/
├── landing/
│   ├── athlete_bio.csv
│   └── athlete_event_results.csv
├── bronze/
│   ├── athlete_bio/
│   │   ├── part-....parquet
│   │   └── _SUCCESS
│   └── athlete_event_results/
│       ├── part-....parquet
│       └── _SUCCESS
├── silver/
│   ├── athlete_bio/
│   │   └── ...
│   └── athlete_event_results/
│       └── ...
└── gold/
    └── avg_stats/
        └── ...
```

Spark writes each Parquet dataset as a directory containing one or more data parts and metadata files; it is not a single `.parquet` file. The `data/` directory is excluded from Git.

