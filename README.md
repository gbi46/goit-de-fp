# Data Engineering Final Project

This repository contains the current implementation for the GoIT Data Engineering final project. It uses PySpark in Docker and currently includes a MySQL reader and the first batch data lake step, Landing to Bronze.

## Current implementation

- `read_mysql.py` reads `athlete_bio` and `athlete_event_results` from MySQL. It casts `height` and `weight` to numbers, filters invalid values from `athlete_bio`, and prints sample rows.
- `landing_to_bronze.py` downloads `athlete_bio.csv` and `athlete_event_results.csv` from `SOURCE_URL`, saves the original CSV files under `data/landing/`, then writes Parquet datasets under `data/bronze/`.
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
./run.sh read_mysql
./run.sh print_json
```

`run.sh` builds or reuses the `spark-mysql-driver:latest` image, then runs the selected script in a temporary container. For `landing_to_bronze`, the local `data/` directory is mounted into the container so downloaded CSV and Parquet output persist on your machine. `read_mysql` uses the MySQL values from `.env`.

`print_json` uses the MySQL settings in `.env` to print a single `athlete_event_results` record as JSON. For Python scripts in the repository root, pass the filename with or without `.py`.

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

