"""
PriceRadar pipeline DAG.

    ingest_bestbuy ─┐
                    ├─> spark_processing ─> sku_matching ─> load_bigquery ─> dbt_build
    ingest_ebay ────┘

Runs every 6 hours. Each task is idempotent enough to retry: ingestion appends a new
snapshot, matching skips pairs it has already judged, the BigQuery load replaces its
tables, and dbt rebuilds the models.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.warehouse import credentials_path  # noqa: E402

DBT_DIR = PROJECT_ROOT / "dbt" / "priceradar"
DBT_BIN = os.getenv("DBT_BIN", "dbt")


def run_bestbuy_ingest() -> int:
    from ingestion.bestbuy_ingest import main

    return main(products_per_category=10)


def run_ebay_ingest() -> int:
    from ingestion.ebay_ingest import main

    return main(max_items_per_keyword=50)


def run_spark_processing() -> None:
    from spark.process import main

    main()


def run_sku_matching() -> dict:
    from llm.sku_matcher import main

    return main()


def run_load_bigquery() -> None:
    from ingestion.load_to_bigquery import main

    main()


default_args = {
    "owner": "priceradar",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="priceradar_ingestion",
    description="Ingest listings, match products across marketplaces, build the warehouse",
    default_args=default_args,
    schedule=timedelta(hours=6),
    start_date=datetime(2026, 3, 18),
    catchup=False,
    max_active_runs=1,
    tags=["priceradar", "ingestion", "pipeline"],
    doc_md=__doc__,
) as dag:
    ingest_bestbuy = PythonOperator(task_id="ingest_bestbuy", python_callable=run_bestbuy_ingest)
    ingest_ebay = PythonOperator(task_id="ingest_ebay", python_callable=run_ebay_ingest)
    spark_processing = PythonOperator(
        task_id="spark_processing", python_callable=run_spark_processing
    )
    sku_matching = PythonOperator(
        task_id="sku_matching",
        python_callable=run_sku_matching,
        execution_timeout=timedelta(minutes=45),
    )
    load_bigquery = PythonOperator(task_id="load_bigquery", python_callable=run_load_bigquery)
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} deps && {DBT_BIN} build --profiles-dir .",
        # dbt's profile reads the key path from this variable.
        env={"GOOGLE_APPLICATION_CREDENTIALS": credentials_path() or ""},
        append_env=True,
    )

    [ingest_bestbuy, ingest_ebay] >> spark_processing >> sku_matching >> load_bigquery >> dbt_build
