# PriceRadar dbt project

Builds the BigQuery star schema from the raw tables that `ingestion/load_to_bigquery.py` loads.

```
staging        stg_listings, stg_matched          (views over the raw tables)
intermediate   int_listing_products               (listing title -> product_key, from LLM matches)
marts          fact_price_snapshot, dim_product, dim_source, dim_time, dim_category
```

Run from this directory. `profiles.yml` reads `GCP_PROJECT_ID`, `BQ_DATASET` and
`GOOGLE_APPLICATION_CREDENTIALS` from the environment.

```bash
dbt deps
dbt build                 # run models + tests
dbt source freshness      # warn if no new listings in 12 hours
```
