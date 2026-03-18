# PriceRadar - Project Context

## What This Is
B2B Competitive Pricing Intelligence Platform. Monitors competitor pricing across
eBay and Best Buy, normalizes product SKUs using an LLM, and provides analytics
dashboards and price forecasting. Educational/portfolio project for NJIT MS Data Science.

## Architecture
```
[eBay Browse API] [Best Buy API]
        │                │
        └───────┬────────┘
                │  Airflow DAG (@every6hours)
                ▼
        [AWS S3 Raw Layer]  ← Timestamped raw JSON
                │
                ▼
        [PostgreSQL Staging] ← raw_listings table
                │
                ▼
        [PySpark Job]       ← Dedup, normalize, clean
                │
        [LLM SKU Matching]  ← Anthropic Claude structured output
                │
                ▼
        [BigQuery Warehouse] ← Star schema via dbt
                │
        ┌───────┴────────┐
        ▼                ▼
  [Metabase]         [MLflow]
  Dashboards         ML experiments
                │
        [FastAPI Service]
```

## Two-Developer Split

### Track A - Upstream Pipeline (Chris)
Files: `ingestion/`, `spark/`, `llm/`, `dags/`, `docker/postgres_init.sql`

### Track B - Downstream Analytics (Punith)
Files: `dbt/`, `ml/`, `api/`, `ingestion/load_to_bigquery.py`

### Shared (coordinate before editing)
`docker-compose.yml`, `requirements.txt`, `.env.example`, `README.md`

## Handoff Contract
Chris writes → Punith reads:
1. PostgreSQL `raw_listings` table (see `docker/postgres_init.sql`)
2. PostgreSQL `matched_products` table (see `docker/postgres_init.sql`)
3. S3 Parquet at `s3://priceradar-raw/processed/clean/date=YYYY-MM-DD/`

## Key Constraints
- eBay API: inference/classification only, NOT training (license compliance)
- Free tier limits: eBay 5K calls/day, Best Buy 5 req/sec, S3 5GB, BQ 1TB queries/mo
- All secrets via environment variables, never hardcode
- Python 3.11+, type hints, Pydantic models, logging (not print)
