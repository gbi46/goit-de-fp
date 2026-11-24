# Data Engineering Final Project

This repository contains the current implementation for the GoIT Data Engineering final project. It uses PySpark in Docker and includes a MySQL reader and batch data lake steps through Bronze to Silver.

## Current implementation

- `read_mysql.py` reads `athlete_bio` and `athlete_event_results` from MySQL. It casts `height` and `weight` to numbers, filters invalid values from `athlete_bio`, and prints sample rows.
- `landing_to_bronze.py` downloads `athlete_bio.csv` and `athlete_event_results.csv` from `SOURCE_URL`, saves the original CSV files under `data/landing/`, then writes Parquet datasets under `data/bronze/`.
- `bronze_to_silver.py` cleans text columns, removes duplicate rows, and writes Parquet datasets under `data/silver/`.
- `run.sh` builds the Docker image and runs one of the supported scripts.
- `Dockerfile` installs Java 17, the Python dependencies, and MySQL Connector/J 8.0.32.

## Requirements

- Docker
- Internet access to build the image and download source CSV files
- MySQL credentials for `read_mysql.py`

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
./run.sh read_mysql
./run.sh print_json
```

`run.sh` builds or reuses the `spark-mysql-driver:latest` image, then runs the selected script in a temporary container. For `landing_to_bronze`, the local `data/` directory is mounted into the container so downloaded CSV and Parquet output persist on your machine. `read_mysql` uses the MySQL values from `.env`.

`print_json` uses the MySQL settings in `.env` to print a single `athlete_event_results` record as JSON. For Python scripts in the repository root, pass the filename with or without `.py`.

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

After a successful Landing to Bronze run, the data directory will look like this:

```text
data/
├── landing/
│   ├── athlete_bio.csv
│   └── athlete_event_results.csv
└── bronze/
    ├── athlete_bio/
    │   ├── part-....parquet
    │   └── _SUCCESS
    └── athlete_event_results/
        ├── part-....parquet
        └── _SUCCESS
```

Spark writes each Parquet dataset as a directory containing one or more data parts and metadata files; it is not a single `.parquet` file. The `data/` directory is excluded from Git.

