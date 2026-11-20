"""Download the source CSV files and write them to the Bronze layer as Parquet."""

import os
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from pyspark.sql import SparkSession


TABLES = ("athlete_bio", "athlete_event_results")


def download_csv(table: str, landing_dir: Path, source_url: str) -> Path:
    """Download one source CSV to the local landing directory."""
    landing_dir.mkdir(parents=True, exist_ok=True)
    csv_path = landing_dir / f"{table}.csv"
    url = f"{source_url}/{table}.csv"

    print(f"Downloading {url}")
    try:
        with urlopen(url, timeout=60) as response:
            csv_path.write_bytes(response.read())
    except (HTTPError, URLError, TimeoutError) as error:
        raise RuntimeError(f"Could not download {url}: {error}") from error

    print(f"Saved CSV to {csv_path}")
    return csv_path


def main() -> None:
    source_url = os.getenv("SOURCE_URL", "").strip().rstrip("/")
    if not source_url:
        print(
            "SOURCE_URL is missing. Add SOURCE_URL "
            "to your .env file and run this script again."
        )
        raise SystemExit(1)

    project_dir = Path(__file__).resolve().parent
    data_dir = Path(os.getenv("DATA_DIR", project_dir / "data")).resolve()
    landing_dir = data_dir / "landing"
    bronze_dir = data_dir / "bronze"

    spark = (
        SparkSession.builder
        .appName("LandingToBronze")
        .getOrCreate()
    )

    try:
        for table in TABLES:
            # Stage 1: download the source file in its original CSV format.
            csv_path = download_csv(table, landing_dir, source_url)

            # Stage 1: read CSV with its header and save the table as Parquet.
            dataframe = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(str(csv_path))
            )

            output_path = bronze_dir / table
            dataframe.write.mode("overwrite").parquet(str(output_path))
            print(f"Wrote {table} Bronze Parquet to {output_path}")
            dataframe.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
