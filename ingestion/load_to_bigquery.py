"""
PriceRadar - Load data from PostgreSQL into BigQuery staging tables.
Owner: Punith (Track B)
"""

import os
import json
import logging

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID', 'priceradar-490621')
BQ_DATASET = 'priceradar_warehouse'

POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'priceradar')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'priceradar')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'priceradar_dev')

DATABASE_URL = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'


def get_bq_client():
    client = bigquery.Client(project=GCP_PROJECT_ID)
    logger.info(f'Connected to BigQuery project: {client.project}')
    return client


def get_pg_engine():
    engine = sa.create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text('SELECT 1'))
    logger.info(f'Connected to PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
    return engine


def ensure_dataset(client):
    dataset_ref = bigquery.DatasetReference(GCP_PROJECT_ID, BQ_DATASET)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f'Dataset {BQ_DATASET} already exists')
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = 'US'
        client.create_dataset(dataset)
        logger.info(f'Created dataset {BQ_DATASET}')


def upload_to_bigquery(client, df, table_name, schema):
    table_id = f'{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}'
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    logger.info(f'Loaded {table.num_rows} rows into {table_id}')


def load_raw_listings(client, engine):
    query = """
        SELECT id, product_name, price, sale_price, on_sale, source,
               url, category, brand, sku, scraped_at,
               raw_payload::text as raw_payload
        FROM raw_listings ORDER BY scraped_at
    """
    df = pd.read_sql(query, engine)
    logger.info(f'Read {len(df)} rows from raw_listings')
    if df.empty:
        logger.warning('No rows in raw_listings - skipping')
        return
    df['price'] = df['price'].astype(float)
    df['sale_price'] = df['sale_price'].astype(float)
    df['on_sale'] = df['on_sale'].astype(bool)
    df['scraped_at'] = pd.to_datetime(df['scraped_at'], utc=True)
    schema = [
        bigquery.SchemaField('id', 'INTEGER'),
        bigquery.SchemaField('product_name', 'STRING'),
        bigquery.SchemaField('price', 'FLOAT'),
        bigquery.SchemaField('sale_price', 'FLOAT'),
        bigquery.SchemaField('on_sale', 'BOOLEAN'),
        bigquery.SchemaField('source', 'STRING'),
        bigquery.SchemaField('url', 'STRING'),
        bigquery.SchemaField('category', 'STRING'),
        bigquery.SchemaField('brand', 'STRING'),
        bigquery.SchemaField('sku', 'STRING'),
        bigquery.SchemaField('scraped_at', 'TIMESTAMP'),
        bigquery.SchemaField('raw_payload', 'STRING'),
    ]
    upload_to_bigquery(client, df, 'raw_listings', schema)


def load_raw_matched(client, engine):
    query = """
        SELECT id, ebay_listing_id, bestbuy_listing_id, canonical_name,
               brand, model_number, confidence_score, is_match, matched_at
        FROM matched_products
    """
    df = pd.read_sql(query, engine)
    logger.info(f'Read {len(df)} rows from matched_products')
    if df.empty:
        logger.warning('No matched_products yet - Chris has not run SKU matching')
        return
    df['confidence_score'] = df['confidence_score'].astype(float)
    df['is_match'] = df['is_match'].astype(bool)
    df['matched_at'] = pd.to_datetime(df['matched_at'], utc=True)
    schema = [
        bigquery.SchemaField('id', 'INTEGER'),
        bigquery.SchemaField('ebay_listing_id', 'INTEGER'),
        bigquery.SchemaField('bestbuy_listing_id', 'INTEGER'),
        bigquery.SchemaField('canonical_name', 'STRING'),
        bigquery.SchemaField('brand', 'STRING'),
        bigquery.SchemaField('model_number', 'STRING'),
        bigquery.SchemaField('confidence_score', 'FLOAT'),
        bigquery.SchemaField('is_match', 'BOOLEAN'),
        bigquery.SchemaField('matched_at', 'TIMESTAMP'),
    ]
    upload_to_bigquery(client, df, 'raw_matched_products', schema)


def main():
    logger.info('=' * 60)
    logger.info('PriceRadar BigQuery Loader - Starting')
    logger.info('=' * 60)
    bq_client = get_bq_client()
    pg_engine = get_pg_engine()
    ensure_dataset(bq_client)
    load_raw_listings(bq_client, pg_engine)
    load_raw_matched(bq_client, pg_engine)
    logger.info('=' * 60)
    logger.info('BigQuery Loader - Complete')
    logger.info('=' * 60)


if __name__ == '__main__':
    main()