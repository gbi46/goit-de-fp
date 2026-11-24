"""Clean Bronze datasets and write deduplicated Silver Parquet datasets."""

import os
import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


TABLES = ("athlete_bio", "athlete_event_results")


def clean_text(value: str | None) -> str | None:
    """Remove unwanted punctuation and control characters from a text value."""
    if value is None:
        return None
    return re.sub(r"[^a-zA-Z0-9,.\\\"' ]", "", value).strip()


clean_text_udf = udf(clean_text, StringType())


def transform_table(spark: SparkSession, table: str, data_dir: Path) -> None:
    source = data_dir / "bronze" / table
    destination = data_dir / "silver" / table
    if not source.exists():
        raise FileNotFoundError(
            f"Bronze dataset not found: {source}. Run landing_to_bronze first."
        )

    dataframe = spark.read.parquet(str(source))
    # Stage 2: clean every text column and remove duplicate rows.
    for field in dataframe.schema.fields:
        if isinstance(field.dataType, StringType):
            dataframe = dataframe.withColumn(field.name, clean_text_udf(col(field.name)))

    dataframe = dataframe.dropDuplicates()
    dataframe.write.mode("overwrite").parquet(str(destination))
    print(f"Wrote Silver dataset to {destination}")
    dataframe.show(10, truncate=False)


def main() -> None:
    project_dir = Path(__file__).resolve().parent
    data_dir = Path(os.getenv("DATA_DIR", project_dir / "data")).resolve()
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
    try:
        for table in TABLES:
            transform_table(spark, table, data_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
