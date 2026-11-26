"""Streaming pipeline: Kafka results + MySQL athlete_bio -> average stats.

Writes every micro-batch to the output Kafka topic and to a MySQL table.
"""

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    expr,
    from_json,
    struct,
    to_json,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from mysql_to_kafka import kafka_options, missing_vars


REQUIRED_VARS = [
    "MYSQL_HOST",
    "MYSQL_PORT",
    "MYSQL_DATABASE",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_INPUT_TOPIC",
    "KAFKA_OUTPUT_TOPIC",
]

# JSON fields written by mysql_to_kafka.py (types follow the MySQL table).
RESULT_SCHEMA = StructType([
    StructField("edition", StringType()),
    StructField("edition_id", IntegerType()),
    StructField("country_noc", StringType()),
    StructField("sport", StringType()),
    StructField("event", StringType()),
    StructField("result_id", LongType()),
    StructField("athlete", StringType()),
    StructField("athlete_id", IntegerType()),
    StructField("pos", StringType()),
    StructField("medal", StringType()),
    StructField("isTeamSport", StringType()),
])

GROUP_COLUMNS = ["sport", "medal", "sex", "country_noc"]

# Source weights contain merged ranges ("210-218" stored as 210218).
# Keep only physically plausible values in kilograms.
MIN_WEIGHT_KG = 20
MAX_WEIGHT_KG = 250


def jdbc_url(database: str) -> str:
    return (
        f"jdbc:mysql://{os.environ['MYSQL_HOST']}:{os.environ['MYSQL_PORT']}"
        f"/{database}?useSSL=false&allowPublicKeyRetrieval=true"
    )


def jdbc_options(database: str, table: str) -> dict[str, str]:
    return {
        "url": jdbc_url(database),
        "dbtable": table,
        "user": os.environ["MYSQL_USER"],
        "password": os.environ["MYSQL_PASSWORD"],
        "driver": "com.mysql.cj.jdbc.Driver",
    }


def main() -> None:
    missing = missing_vars(REQUIRED_VARS)
    if missing:
        raise SystemExit(f"Set these variables in .env: {', '.join(missing)}")

    source_db = os.environ["MYSQL_DATABASE"]
    output_db = os.getenv("OUTPUT_MYSQL_DATABASE", "neo_data")
    output_table = os.getenv("OUTPUT_MYSQL_TABLE", "athlete_enriched_agg")
    output_topic = os.environ["KAFKA_OUTPUT_TOPIC"]
    data_dir = Path(
        os.getenv("DATA_DIR", Path(__file__).resolve().parent / "data")
    )
    # Offsets of the remote and the local broker must not share a checkpoint.
    kafka_target = os.getenv("KAFKA_TARGET", "remote")
    checkpoint_dir = data_dir / "checkpoints" / f"streaming_pipeline_{kafka_target}"

    jars = ",".join(
        path
        for path in (os.getenv("MYSQL_JDBC_JAR"), os.getenv("KAFKA_JARS"))
        if path
    )
    spark = (
        SparkSession.builder
        .appName("AthleteStreamingPipeline")
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Етап 1: зчитування фізичних показників атлетів з MySQL (athlete_bio).
    athlete_bio = (
        spark.read
        .format("jdbc")
        .options(**jdbc_options(source_db, "athlete_bio"))
        .load()
    )

    # Етап 2: фільтрація рядків, де зріст або вага порожні чи не є числами,
    # а також ваги поза реальними межами.
    athlete_bio = (
        athlete_bio
        .select(
            col("athlete_id"),
            col("sex"),
            expr("try_cast(height AS DOUBLE)").alias("height"),
            expr("try_cast(weight AS DOUBLE)").alias("weight"),
        )
        .filter(
            col("athlete_id").isNotNull()
            & col("height").isNotNull()
            & col("weight").isNotNull()
            & col("weight").between(MIN_WEIGHT_KG, MAX_WEIGHT_KG)
        )
        .cache()
    )

    # Етап 3: зчитування результатів змагань з Kafka-топіку.
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options())
        .option("subscribe", os.environ["KAFKA_INPUT_TOPIC"])
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "50000")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Етап 3: JSON з value -> окремі колонки датафрейму.
    results = (
        kafka_stream
        .select(from_json(col("value").cast("string"), RESULT_SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("athlete_id").isNotNull())
    )

    # Етап 4: об'єднання результатів з біологічними даними за athlete_id.
    enriched = results.select(
        "athlete_id", "sport", "medal", "country_noc"
    ).join(athlete_bio, on="athlete_id", how="inner")

    # Етап 5: середній зріст і вага для кожної комбінації
    # sport, medal (або її відсутності), sex, country_noc.
    aggregated = enriched.groupBy(*GROUP_COLUMNS).agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
    )

    def foreach_batch_function(batch_df: DataFrame, batch_id: int) -> None:
        # Етап 5: timestamp, коли розрахунки були зроблені.
        batch_df = batch_df.withColumn(
            "timestamp", current_timestamp()
        ).cache()
        if batch_df.isEmpty():
            batch_df.unpersist()
            return

        # Етап 6.а): запис у вихідний Kafka-топік.
        (
            batch_df
            .select(to_json(struct(*batch_df.columns)).alias("value"))
            .write
            .format("kafka")
            .options(**kafka_options())
            .option("topic", output_topic)
            .save()
        )

        # Етап 6.b): запис у базу даних MySQL.
        (
            batch_df.write
            .format("jdbc")
            .options(**jdbc_options(output_db, output_table))
            .mode("append")
            .save()
        )

        print(f"Batch {batch_id}: wrote {batch_df.count()} rows")
        batch_df.show(10, truncate=False)
        batch_df.unpersist()

    # Етап 6: стрим даних через forEachBatch у Kafka і MySQL.
    query = (
        aggregated.writeStream
        .foreachBatch(foreach_batch_function)
        .outputMode("update")
        .option("checkpointLocation", str(checkpoint_dir))
        .start()
    )
    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
