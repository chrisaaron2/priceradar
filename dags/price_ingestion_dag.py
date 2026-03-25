"""
PriceRadar - Airflow DAG (Production-Ready)

Orchestrates the full data pipeline end-to-end:
1. Ingest from eBay + Best Buy (parallel)
2. Spark processing (dedup, normalize, clean Parquet)
3. LLM SKU matching via Groq/Llama 3.3 70B
4. Load to BigQuery (Punith's code)
5. dbt run (Punith's star schema models)

Schedule: Every 6 hours
Owner: Chris (Track A)
Tasks 4-5 call Punith's Track B code.
"""

from datetime import datetime, timedelta
from pathlib import Path
import subprocess
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Add project root to path so we can import our modules
PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ---------------------------------------------------------------------------
# DAG Default Arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "chris",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Task Callables — Track A (Chris)
# ---------------------------------------------------------------------------
def run_bestbuy_ingest():
    """Run synthetic Best Buy data generation."""
    from ingestion.bestbuy_ingest import main
    main(products_per_category=10)


def run_ebay_ingest():
    """Run eBay Browse API ingestion with brand-specific searches."""
    from ingestion.ebay_ingest import main
    main(max_items_per_keyword=50)


def run_spark_processing():
    """Run PySpark dedup, normalize, and clean Parquet output."""
    from spark.process import main
    main()


def run_sku_matching():
    """Run LLM SKU matching via Groq/Llama 3.3 70B."""
    from llm.sku_matcher import main
    main(max_pairs=50)


# ---------------------------------------------------------------------------
# Task Callables — Track B (Punith)
# ---------------------------------------------------------------------------
def run_load_bigquery():
    """
    Load data from PostgreSQL to BigQuery.
    Calls Punith's ingestion/load_to_bigquery.py.
    """
    from ingestion.load_to_bigquery import main
    main()


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="priceradar_ingestion",
    default_args=default_args,
    description="PriceRadar: ingest → process → match → load → transform",
    schedule=timedelta(hours=6),
    start_date=datetime(2026, 3, 18),
    catchup=False,
    tags=["priceradar", "ingestion", "pipeline"],
    doc_md="""
    ## PriceRadar Data Pipeline

    **Schedule:** Every 6 hours

    **Task Flow:**
    ```
    [ingest_bestbuy] ─┐
                       ├─→ [spark_processing] → [sku_matching] → [load_bigquery] → [dbt_run]
    [ingest_ebay] ────┘
    ```

    **Track A (Chris):** Tasks 1-3 (ingestion, Spark, LLM matching)
    **Track B (Punith):** Tasks 4-5 (BigQuery loading, dbt models)
    """,
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 & 2: Ingestion (parallel) — Chris
    # -----------------------------------------------------------------------
    ingest_bestbuy = PythonOperator(
        task_id="ingest_bestbuy",
        python_callable=run_bestbuy_ingest,
        doc_md="Generate synthetic Best Buy product listings → S3 + Postgres",
    )

    ingest_ebay = PythonOperator(
        task_id="ingest_ebay",
        python_callable=run_ebay_ingest,
        doc_md="Fetch live eBay product listings via Browse API → S3 + Postgres",
    )

    # -----------------------------------------------------------------------
    # Task 3: Spark Processing — Chris
    # -----------------------------------------------------------------------
    spark_processing = PythonOperator(
        task_id="spark_processing",
        python_callable=run_spark_processing,
        doc_md="PySpark: dedup, normalize prices/categories, output clean Parquet to S3",
    )

    # -----------------------------------------------------------------------
    # Task 4: LLM SKU Matching — Chris
    # -----------------------------------------------------------------------
    sku_matching = PythonOperator(
        task_id="sku_matching",
        python_callable=run_sku_matching,
        doc_md="Groq/Llama 3.3 70B cross-marketplace product matching with Pydantic structured output",
    )

    # -----------------------------------------------------------------------
    # Task 5: Load to BigQuery — Punith
    # -----------------------------------------------------------------------
    load_bigquery = PythonOperator(
        task_id="load_bigquery",
        python_callable=run_load_bigquery,
        doc_md="Load raw_listings + matched_products from Postgres to BigQuery staging tables",
    )

    # -----------------------------------------------------------------------
    # Task 6: dbt Run — Punith
    # -----------------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /usr/local/airflow/dbt/priceradar && dbt run --full-refresh",
        doc_md="Run dbt models to build star schema in BigQuery (fact + dim tables)",
    )

    # -----------------------------------------------------------------------
    # Task Dependencies
    # -----------------------------------------------------------------------
    [ingest_bestbuy, ingest_ebay] >> spark_processing >> sku_matching >> load_bigquery >> dbt_run
