"""
PriceRadar - Airflow DAG

Orchestrates the full data pipeline:
1. Ingest from eBay + Best Buy (parallel)
2. Spark processing (dedup, normalize, clean Parquet)
3. SKU matching via LLM (Day 3 — placeholder)
4. Load to BigQuery (Punith's code — placeholder)
5. dbt run (Punith's models — placeholder)

Schedule: Every 6 hours
Owner: Chris (Track A)

Tasks 3-5 are placeholder operators that will be enabled as the
corresponding code is ready. This lets us test the DAG structure
now and incrementally enable tasks.
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

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
# Task Callables
# ---------------------------------------------------------------------------
def run_bestbuy_ingest():
    """Run synthetic Best Buy data generation."""
    from ingestion.bestbuy_ingest import main
    main(products_per_category=10)


def run_ebay_ingest():
    """Run eBay Browse API ingestion."""
    from ingestion.ebay_ingest import main
    main(max_items_per_category=200)


def run_spark_processing():
    """Run PySpark dedup, normalize, and clean Parquet output."""
    from spark.process import main
    main()


# Placeholder callables for future tasks
def run_sku_matching():
    """Placeholder: LLM SKU matching (Day 3)."""
    print("SKU matching task — not yet implemented. Enable in Day 3.")


def run_load_bigquery():
    """Placeholder: BigQuery loading (Punith's code)."""
    print("BigQuery load task — Punith's ingestion/load_to_bigquery.py. Enable when ready.")


def run_dbt():
    """Placeholder: dbt run (Punith's models)."""
    print("dbt run task — Punith's dbt models. Enable when ready.")


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="priceradar_ingestion",
    default_args=default_args,
    description="PriceRadar data pipeline: ingest → process → match → load → transform",
    schedule=timedelta(hours=6),
    start_date=datetime(2026, 3, 18),
    catchup=False,  # Don't backfill past runs
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

    **Owners:**
    - Tasks 1-3: Chris (Track A — Pipeline)
    - Tasks 4-5: Punith (Track B — Analytics)
    """,
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 & 2: Ingestion (parallel)
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
    # Task 3: Spark Processing (after both ingestions)
    # -----------------------------------------------------------------------
    spark_processing = PythonOperator(
        task_id="spark_processing",
        python_callable=run_spark_processing,
        doc_md="PySpark: dedup, normalize prices/categories, output clean Parquet to S3",
    )

    # -----------------------------------------------------------------------
    # Task 4: LLM SKU Matching (Day 3 — placeholder)
    # -----------------------------------------------------------------------
    sku_matching = PythonOperator(
        task_id="sku_matching",
        python_callable=run_sku_matching,
        doc_md="LLM cross-marketplace product matching (Day 3)",
    )

    # -----------------------------------------------------------------------
    # Task 5: Load to BigQuery (Punith's code — placeholder)
    # -----------------------------------------------------------------------
    load_bigquery = PythonOperator(
        task_id="load_bigquery",
        python_callable=run_load_bigquery,
        doc_md="Load clean Parquet + matched products to BigQuery (Punith)",
    )

    # -----------------------------------------------------------------------
    # Task 6: dbt Run (Punith's models — placeholder)
    # -----------------------------------------------------------------------
    dbt_run = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt,
        doc_md="Run dbt models to build star schema in BigQuery (Punith)",
    )

    # -----------------------------------------------------------------------
    # Task Dependencies
    # -----------------------------------------------------------------------
    # Ingestion runs in parallel, then Spark, then sequential downstream
    [ingest_bestbuy, ingest_ebay] >> spark_processing >> sku_matching >> load_bigquery >> dbt_run
