# PriceRadar - project context

PriceRadar is the data pipeline behind ShopGPT: it tracks consumer-electronics prices on
eBay (live Browse API) and Best Buy (generated from a real-product catalog), uses an LLM
to work out which listings are the same product, and builds a BigQuery star schema that
Metabase, the FastAPI service and the ML jobs read.

## Flow
Airflow DAG `priceradar_ingestion`, every 6 hours:
`ingest_bestbuy + ingest_ebay -> spark_processing -> sku_matching -> load_bigquery -> dbt_build`

- `ingestion/` writes raw JSON to S3 and rows to Postgres `raw_listings`
- `spark/process.py` cleans raw_listings into Parquet on S3
- `llm/sku_matcher.py` judges eBay/Best Buy title pairs into Postgres `matched_products`
- `ingestion/load_to_bigquery.py` mirrors both tables into BigQuery
- `dbt/priceradar` builds `int_listing_products` (title -> product) and the star schema;
  `dim_product` merges listings that the LLM matched with confidence >= 0.6
- `api/` (ShopGPT API) and `ml/` read the star schema
- `common/` holds shared Postgres, S3, BigQuery and logging helpers

## Conventions
- Python 3.11, type hints, `common.log.get_logger`, no print
- All secrets come from environment variables (.env, see .env.example); never hardcode
- Postgres timestamps are naive UTC
- eBay data is for inference/classification only, never training (eBay API license)
- Run `ruff check . && ruff format --check . && pytest` before committing
- Changing `raw_listings` or `matched_products` means updating `docker/postgres_init.sql`,
  `ingestion/load_to_bigquery.py` and the dbt staging models together
