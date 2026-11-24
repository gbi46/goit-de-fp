"""Join the Silver datasets and write grouped athlete measurements to Gold."""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp, expr


REQUIRED_BIO_COLUMNS = {"athlete_id", "height", "weight"}
REQUIRED_RESULT_COLUMNS = {
    "athlete_id",
    "sport",
    "medal",
    "country_noc",
}
GROUP_COLUMNS = ["sport", "medal", "country_noc"]


def build_gold(spark: SparkSession, data_dir: Path) -> None:
    """Create the average height and weight for each required grouping."""
    bio_path = data_dir / "silver" / "athlete_bio"
    results_path = data_dir / "silver" / "athlete_event_results"
    output_path = data_dir / "gold" / "avg_stats"

    for dataset_path in (bio_path, results_path):
        if not dataset_path.exists():
            raise FileNotFoundError(
                f"Silver dataset not found: {dataset_path}. "
                "Run bronze_to_silver first."
            )

    bio = spark.read.parquet(str(bio_path))
    results = spark.read.parquet(str(results_path))

    missing_bio = REQUIRED_BIO_COLUMNS - set(bio.columns)
    missing_results = REQUIRED_RESULT_COLUMNS - set(results.columns)
    if missing_bio or missing_results:
        details = []
        if missing_bio:
            details.append(f"athlete_bio is missing {sorted(missing_bio)}")
        if missing_results:
            details.append(
                f"athlete_event_results is missing {sorted(missing_results)}"
            )
        raise ValueError("Invalid Silver schema: " + "; ".join(details))

    # Stage 3: cast measurements to numbers and discard missing/invalid values.
    bio = (
        bio.select(
            col("athlete_id").alias("bio_athlete_id"),
            expr("try_cast(height AS DOUBLE)").alias("height"),
            expr("try_cast(weight AS DOUBLE)").alias("weight"),
        )
        .filter(
            col("bio_athlete_id").isNotNull()
            & col("height").isNotNull()
            & col("weight").isNotNull()
        )
    )
    results = results.select(
        col("athlete_id").alias("result_athlete_id"),
        *[col(name) for name in GROUP_COLUMNS],
    )

    # Stage 3: join on athlete_id and aggregate by sport, medal and country.
    joined = bio.join(
        results,
        bio.bio_athlete_id == results.result_athlete_id,
        "inner",
    )
    gold = (
        joined.groupBy(*GROUP_COLUMNS)
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
        )
        .withColumn("timestamp", current_timestamp())
        .select(
            *GROUP_COLUMNS,
            "avg_height",
            "avg_weight",
            "timestamp",
        )
    )

    # Stage 3: persist the final analytical dataset and show it in the job logs.
    gold.write.mode("overwrite").parquet(str(output_path))
    print(f"Wrote Gold dataset to {output_path}")
    gold.show(20, truncate=False)


def main() -> None:
    project_dir = Path(__file__).resolve().parent
    data_dir = Path(os.getenv("DATA_DIR", project_dir / "data")).resolve()
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
    try:
        build_gold(spark, data_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
