"""Print the newest messages of the output Kafka topic as a table."""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from mysql_to_kafka import kafka_options, missing_vars


OUTPUT_SCHEMA = StructType([
    StructField("sport", StringType()),
    StructField("medal", StringType()),
    StructField("sex", StringType()),
    StructField("country_noc", StringType()),
    StructField("avg_height", DoubleType()),
    StructField("avg_weight", DoubleType()),
    StructField("timestamp", StringType()),
])


def main() -> None:
    missing = missing_vars(["KAFKA_BOOTSTRAP_SERVERS", "KAFKA_OUTPUT_TOPIC"])
    if missing:
        raise SystemExit(f"Set these variables in .env: {', '.join(missing)}")
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    topic = os.environ["KAFKA_OUTPUT_TOPIC"]

    spark = (
        SparkSession.builder
        .appName("ReadOutputTopic")
        .config("spark.jars", os.getenv("KAFKA_JARS", ""))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        messages = (
            spark.read
            .format("kafka")
            .options(**kafka_options())
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
            .select(
                "partition",
                "offset",
                from_json(col("value").cast("string"), OUTPUT_SCHEMA).alias("data"),
            )
            .select("partition", "offset", "data.*")
            .cache()
        )
        print(f"Kafka topic {topic}: {messages.count()} messages")
        messages.orderBy(desc("timestamp"), desc("offset")).show(rows, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
