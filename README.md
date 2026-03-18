# PriceRadar

**B2B Competitive Pricing Intelligence Platform**

> Monitors competitor pricing across eBay and Best Buy, normalizes product SKUs using LLM-powered matching, and surfaces pricing intelligence through dashboards and ML models.

🚧 **Under active development** — Demo target: March 26, 2026 (NJIT AI Exploration Day)

## Quick Start

```bash
# Clone and configure
git clone https://github.com/YOUR_USERNAME/priceradar.git
cd priceradar
cp .env.example .env
# Fill in your API keys in .env

# Start infrastructure
docker-compose up -d

# Run ingestion (after API keys are configured)
python -m ingestion.bestbuy_ingest
python -m ingestion.ebay_ingest
```

## Architecture

_Full architecture diagram and documentation coming in Day 6-7._

## Team

- **Chris Aaron** — Track A: Upstream Pipeline (ingestion, Spark, LLM matching, Airflow)
- **Punith** — Track B: Downstream Analytics (BigQuery, dbt, Metabase, ML, FastAPI)

## License

MIT
