-- PriceRadar PostgreSQL Schema
-- Owner: Chris (Track A)
-- Consumers: Punith (Track B) reads raw_listings + matched_products
--
-- HANDOFF CONTRACT: Do not change these schemas without coordinating
-- with Punith via the management thread.

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

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_raw_listings_source ON raw_listings(source);
CREATE INDEX IF NOT EXISTS idx_raw_listings_category ON raw_listings(category);
CREATE INDEX IF NOT EXISTS idx_raw_listings_scraped_at ON raw_listings(scraped_at);
CREATE INDEX IF NOT EXISTS idx_raw_listings_product_source ON raw_listings(product_name, source);
CREATE INDEX IF NOT EXISTS idx_matched_products_confidence ON matched_products(confidence_score);
CREATE INDEX IF NOT EXISTS idx_matched_products_is_match ON matched_products(is_match);
