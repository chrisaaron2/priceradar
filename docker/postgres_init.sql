-- PriceRadar PostgreSQL Schema
-- Runs once, when the Postgres volume is first created.
-- raw_listings and matched_products are the contract between the pipeline and the
-- warehouse loader; change them together with ingestion/load_to_bigquery.py.

CREATE TABLE IF NOT EXISTS raw_listings (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    price DECIMAL(10,2),
    sale_price DECIMAL(10,2),
    on_sale BOOLEAN DEFAULT FALSE,
    source VARCHAR(20) NOT NULL,          -- "ebay" or "bestbuy"
    url TEXT,
    category TEXT,
    brand TEXT,
    sku VARCHAR(100),
    scraped_at TIMESTAMP DEFAULT NOW(),
    raw_payload JSONB
);

CREATE TABLE IF NOT EXISTS matched_products (
    id SERIAL PRIMARY KEY,
    ebay_listing_id INTEGER REFERENCES raw_listings(id),
    bestbuy_listing_id INTEGER REFERENCES raw_listings(id),
    canonical_name TEXT NOT NULL,
    brand TEXT,
    model_number TEXT,
    confidence_score FLOAT NOT NULL,      -- 0.0 to 1.0
    is_match BOOLEAN NOT NULL,
    matched_at TIMESTAMP DEFAULT NOW()
);

-- Products users asked the API to watch.
CREATE TABLE IF NOT EXISTS tracked_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
    UNIQUE (product_name, category)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_raw_listings_source ON raw_listings(source);
CREATE INDEX IF NOT EXISTS idx_raw_listings_category ON raw_listings(category);
CREATE INDEX IF NOT EXISTS idx_raw_listings_scraped_at ON raw_listings(scraped_at);
CREATE INDEX IF NOT EXISTS idx_raw_listings_product_source ON raw_listings(product_name, source);
CREATE INDEX IF NOT EXISTS idx_matched_products_confidence ON matched_products(confidence_score);
CREATE INDEX IF NOT EXISTS idx_matched_products_is_match ON matched_products(is_match);
CREATE INDEX IF NOT EXISTS idx_matched_products_pair ON matched_products(ebay_listing_id, bestbuy_listing_id);
