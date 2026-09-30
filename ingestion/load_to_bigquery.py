"""
PriceRadar - Postgres to BigQuery loader.

Copies the two staging tables that dbt builds from:
    raw_listings      -> <dataset>.raw_listings
    matched_products  -> <dataset>.raw_matched_products

Each run replaces both BigQuery tables (WRITE_TRUNCATE), so the warehouse always
mirrors Postgres exactly and reruns are safe.
"""

from __future__ import annotations

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery

from common.db import get_engine
from common.log import get_logger
from common.warehouse import bq_client, bq_dataset

load_dotenv()
logger = get_logger("load_to_bigquery")

LISTINGS_QUERY = """
    SELECT id, product_name, price, sale_price, on_sale, source,
           url, category, brand, sku, scraped_at,
           raw_payload::text AS raw_payload
    FROM raw_listings
    ORDER BY id
"""

MATCHES_QUERY = """
    SELECT id, ebay_listing_id, bestbuy_listing_id, canonical_name,
           brand, model_number, confidence_score, is_match, matched_at
    FROM matched_products
    ORDER BY id
"""

LISTINGS_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("sale_price", "FLOAT"),
    bigquery.SchemaField("on_sale", "BOOLEAN"),
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("category", "STRING"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("sku", "STRING"),
    bigquery.SchemaField("scraped_at", "TIMESTAMP"),
    bigquery.SchemaField("raw_payload", "STRING"),
]

MATCHES_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ebay_listing_id", "INTEGER"),
    bigquery.SchemaField("bestbuy_listing_id", "INTEGER"),
    bigquery.SchemaField("canonical_name", "STRING"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("model_number", "STRING"),
    bigquery.SchemaField("confidence_score", "FLOAT"),
    bigquery.SchemaField("is_match", "BOOLEAN"),
    bigquery.SchemaField("matched_at", "TIMESTAMP"),
]


def ensure_dataset(client: bigquery.Client) -> None:
    dataset = bigquery.Dataset(f"{client.project}.{bq_dataset()}")
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)


def upload(client: bigquery.Client, df: pd.DataFrame, table_name: str, schema: list) -> int:
    table_id = f"{client.project}.{bq_dataset()}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    logger.info("Loaded %d rows into %s", len(df), table_id)
    return len(df)


def load_listings(client: bigquery.Client) -> int:
    df = pd.read_sql(LISTINGS_QUERY, get_engine())
    df["price"] = df["price"].astype(float)
    df["sale_price"] = df["sale_price"].astype(float)
    df["on_sale"] = df["on_sale"].fillna(False).astype(bool)
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True)
    return upload(client, df, "raw_listings", LISTINGS_SCHEMA)


def load_matches(client: bigquery.Client) -> int:
    df = pd.read_sql(MATCHES_QUERY, get_engine())
    # Keep integer columns nullable instead of letting pandas turn them into floats.
    for col in ("ebay_listing_id", "bestbuy_listing_id"):
        df[col] = df[col].astype("Int64")
    df["confidence_score"] = df["confidence_score"].astype(float)
    df["is_match"] = df["is_match"].astype(bool)
    df["matched_at"] = pd.to_datetime(df["matched_at"], utc=True)
    # An empty table is still loaded so dbt always finds the source table.
    return upload(client, df, "raw_matched_products", MATCHES_SCHEMA)


def main() -> None:
    client = bq_client()
    ensure_dataset(client)
    listings = load_listings(client)
    matches = load_matches(client)
    logger.info("BigQuery load complete: %d listings, %d match verdicts", listings, matches)


if __name__ == "__main__":
    main()
