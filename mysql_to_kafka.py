"""Read athlete_event_results from MySQL and write each row as JSON to Kafka."""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json


REQUIRED_VARS = [
    "MYSQL_HOST",
    "MYSQL_PORT",
    "MYSQL_DATABASE",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_INPUT_TOPIC",
]
SASL_VARS = ["KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD"]


def uses_sasl() -> bool:
    """The remote cluster uses SASL; the local fallback broker is PLAINTEXT."""
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
    return protocol.upper().startswith("SASL")


def missing_vars(required: list[str]) -> list[str]:
    names = required + (SASL_VARS if uses_sasl() else [])
    return [name for name in names if not os.getenv(name)]


def kafka_options() -> dict[str, str]:
    """Spark Kafka options; PLAIN SASL authentication for the remote cluster."""
    options = {
        "kafka.bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "kafka.security.protocol": os.getenv(
            "KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"
        ),
    }
    if not uses_sasl():
        return options

    username = os.environ["KAFKA_SASL_USERNAME"].replace('"', '\\"')
    password = os.environ["KAFKA_SASL_PASSWORD"].replace('"', '\\"')
    options.update({
        "kafka.sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM", "PLAIN"),
        "kafka.sasl.jaas.config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{username}" password="{password}";'
        ),
    })
    return options


def main() -> None:
    missing = missing_vars(REQUIRED_VARS)
    if missing:
        raise SystemExit(f"Set these variables in .env: {', '.join(missing)}")

    jars = ",".join(
        path
        for path in (os.getenv("MYSQL_JDBC_JAR"), os.getenv("KAFKA_JARS"))
        if path
    )
    spark = (
        SparkSession.builder
        .appName("MySQLToKafka")
        .config("spark.jars", jars)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        jdbc_url = (
            f"jdbc:mysql://{os.environ['MYSQL_HOST']}:{os.environ['MYSQL_PORT']}"
            f"/{os.environ['MYSQL_DATABASE']}"
            "?useSSL=false&allowPublicKeyRetrieval=true"
        )
        topic = os.environ["KAFKA_INPUT_TOPIC"]

        # Етап 3: зчитування athlete_event_results з MySQL.
        results = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "athlete_event_results")
            .option("user", os.environ["MYSQL_USER"])
            .option("password", os.environ["MYSQL_PASSWORD"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )

        # Етап 3: кожен рядок як JSON у value, athlete_id як key.
        messages = results.select(
            col("athlete_id").cast("string").alias("key"),
            to_json(struct(*results.columns)).alias("value"),
        ).cache()

        # Етап 3: запис у Kafka-топік athlete_event_results.
        (
            messages.write
            .format("kafka")
            .options(**kafka_options())
            .option("topic", topic)
            .save()
        )

        print(f"Wrote {messages.count()} messages to Kafka topic {topic}")
        messages.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
