"""Airflow DAG that runs the three batch data lake Spark jobs in order."""

import os
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


DAG_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", DAG_DIR / "data")).resolve()
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark-default")


with DAG(
    dag_id="athlete_batch_data_lake",
    description="Load athlete data through Bronze, Silver and Gold layers",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["goit", "data-engineering", "batch"],
) as dag:
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=str(DAG_DIR / "landing_to_bronze.py"),
        conn_id=SPARK_CONN_ID,
        verbose=True,
        env_vars={
            "DATA_DIR": str(DATA_DIR),
            "SOURCE_URL": os.getenv("SOURCE_URL", ""),
        },
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=str(DAG_DIR / "bronze_to_silver.py"),
        conn_id=SPARK_CONN_ID,
        verbose=True,
        env_vars={"DATA_DIR": str(DATA_DIR)},
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=str(DAG_DIR / "silver_to_gold.py"),
        conn_id=SPARK_CONN_ID,
        verbose=True,
        env_vars={"DATA_DIR": str(DATA_DIR)},
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
