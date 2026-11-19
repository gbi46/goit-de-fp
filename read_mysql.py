import os

from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


load_dotenv()

required_vars = [
    "MYSQL_HOST",
    "MYSQL_PORT",
    "MYSQL_DATABASE",
    "MYSQL_USER",
    "MYSQL_PASSWORD",
]

missing_vars = [name for name in required_vars if not os.getenv(name)]
if missing_vars:
    raise RuntimeError(
        f"environments variables were not set: {', '.join(missing_vars)}"
    )

jdbc_jar = os.getenv(
    "MYSQL_JDBC_JAR",
    "/opt/jdbc/mysql-connector-j-8.0.32.jar",
)

spark = (
    SparkSession.builder
    .appName("ReadAthleteBioFromMySQL")
    .config("spark.jars", jdbc_jar)
    .getOrCreate()
)

try:
    host = os.environ["MYSQL_HOST"]
    port = os.environ["MYSQL_PORT"]
    database = os.environ["MYSQL_DATABASE"]

    jdbc_url = (
        f"jdbc:mysql://{host}:{port}/{database}"
        "?useSSL=false&allowPublicKeyRetrieval=true"
    )

    # read athlete data from MySQL
    athlete_bio = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "athlete_bio")
        .option("user", os.environ["MYSQL_USER"])
        .option("password", os.environ["MYSQL_PASSWORD"])
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    )

    # transform height and weight to numbers and filtering not correct values
    athlete_bio = (
        athlete_bio
        .withColumn("height", F.expr("try_cast(height AS DOUBLE)"))
        .withColumn("weight", F.expr("try_cast(weight AS DOUBLE)"))
        .filter(
            F.col("height").isNotNull()
            & F.col("weight").isNotNull()
        )
    )

    print("athlete_bio scheme after cleaned:")
    athlete_bio.printSchema()
    athlete_bio.show(10, truncate=False)

    # read competition results from mysql
    athlete_event_results = (
    spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "athlete_event_results")
        .option("user", os.environ["MYSQL_USER"])
        .option("password", os.environ["MYSQL_PASSWORD"])
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
)

    print("scheme athlete_event_results:")
    athlete_event_results.printSchema()
    athlete_event_results.show(10, truncate=False)

finally:
    spark.stop()