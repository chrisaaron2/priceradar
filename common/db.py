"""Postgres access shared by ingestion, matching, loading and the API."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping
from functools import lru_cache
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def postgres_url() -> str:
    """Build the SQLAlchemy URL from POSTGRES_* environment variables."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "priceradar")
    user = os.getenv("POSTGRES_USER", "priceradar")
    password = os.getenv("POSTGRES_PASSWORD", "priceradar_dev")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """One pooled engine per process; pre-ping drops stale connections."""
    return create_engine(postgres_url(), pool_pre_ping=True, future=True)


INSERT_LISTING_SQL = text(
    """
    INSERT INTO raw_listings
        (product_name, price, sale_price, on_sale, source,
         url, category, brand, sku, scraped_at, raw_payload)
    VALUES
        (:product_name, :price, :sale_price, :on_sale, :source,
         :url, :category, :brand, :sku,
         COALESCE(CAST(:scraped_at AS timestamptz) AT TIME ZONE 'UTC',
                  NOW() AT TIME ZONE 'UTC'),
         CAST(:raw_payload AS jsonb))
    """
)


def _listing_params(listing: Mapping[str, Any]) -> dict[str, Any]:
    scraped_at = listing.get("scraped_at")
    return {
        "product_name": listing["product_name"],
        "price": listing.get("price"),
        "sale_price": listing.get("sale_price"),
        "on_sale": bool(listing.get("on_sale", False)),
        "source": listing["source"],
        "url": listing.get("url"),
        "category": listing.get("category"),
        "brand": listing.get("brand"),
        "sku": listing.get("sku"),
        "scraped_at": scraped_at.isoformat() if hasattr(scraped_at, "isoformat") else scraped_at,
        "raw_payload": json.dumps(listing.get("raw_payload") or {}, default=str),
    }


def insert_listings(
    listings: Iterable[Mapping[str, Any]],
    engine: Engine | None = None,
    batch_size: int = 1000,
) -> int:
    """Insert listings into raw_listings in batches inside one transaction.

    Timestamps are stored as naive UTC. A listing without `scraped_at` gets NOW().
    """
    rows = [_listing_params(listing) for listing in listings]
    if not rows:
        return 0
    engine = engine or get_engine()
    with engine.begin() as conn:
        for start in range(0, len(rows), batch_size):
            conn.execute(INSERT_LISTING_SQL, rows[start : start + batch_size])
    return len(rows)
