"""
PriceRadar / ShopGPT API.

Read endpoints query the BigQuery star schema built by dbt. Tracking requests are
stored in Postgres (`tracked_products`).

    GET  /health
    GET  /products                       search products, with the current best price
    GET  /products/{product_key}         current offer from each marketplace, cheapest first
    GET  /products/{product_key}/history daily lowest price per marketplace
    GET  /price-history/{name}           price snapshots for products matching a name
    GET  /anomalies                      recent unusual price drops
    POST /track-product                  ask PriceRadar to watch a product
    GET  /tracked-products

Set API_KEY to require an `X-API-Key` header on every endpoint except /health.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
import datetime as dt
from typing import Annotated

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import text

from common.db import get_engine
from common.log import get_logger
from common.warehouse import bq_client, table

load_dotenv()
logger = get_logger("api")

TRACKED_PRODUCTS_DDL = """
CREATE TABLE IF NOT EXISTS tracked_products (
    id SERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
    UNIQUE (product_name, category)
)
"""


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        with get_engine().begin() as conn:
            conn.execute(text(TRACKED_PRODUCTS_DDL))
    except Exception:
        logger.exception("Could not ensure tracked_products table; /track-product will fail")
    yield


app = FastAPI(
    title="ShopGPT API",
    description="Cross-marketplace electronics prices from the PriceRadar pipeline.",
    version="2.0.0",
    lifespan=lifespan,
)

_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def require_api_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
    expected = os.getenv("API_KEY")
    if expected and x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key header")


Protected = [Depends(require_api_key)]


# Response models
class HealthResponse(BaseModel):
    status: str
    timestamp: dt.datetime
    bigquery_connected: bool
    postgres_connected: bool


class ProductSummary(BaseModel):
    product_key: str
    canonical_name: str
    brand: str | None
    category: str | None
    model_number: str | None
    source_count: int
    is_cross_marketplace: bool
    best_price: float | None = None
    best_source: str | None = None
    last_seen: dt.datetime | None = None


class Offer(BaseModel):
    marketplace: str
    display_name: str
    price: float
    effective_price: float
    on_sale: bool
    listing_url: str | None
    seen_at: dt.datetime


class ProductDetail(ProductSummary):
    offers: list[Offer]
    savings_vs_highest: float | None = Field(
        None, description="Highest current price minus the lowest one"
    )


class DailyPrice(BaseModel):
    date: dt.date
    marketplace: str
    lowest_price: float


class PriceSnapshot(BaseModel):
    date: dt.date
    product: str
    price: float
    source: str
    was_on_sale: bool


class PriceHistoryResponse(BaseModel):
    query: str
    snapshots: list[PriceSnapshot]


class Anomaly(BaseModel):
    product: str
    source: str | None = None
    price: float
    expected_price: float
    drop_pct: float
    date: dt.date


class AnomaliesResponse(BaseModel):
    method: str
    anomalies: list[Anomaly]


class TrackProductRequest(BaseModel):
    product_name: str = Field(min_length=2, max_length=300)
    category: str = Field(min_length=2, max_length=50)


class TrackedProduct(BaseModel):
    id: int
    product_name: str
    category: str
    created_at: dt.datetime


# BigQuery helper
def run_query(sql: str, params: dict | None = None) -> list[dict]:
    from google.cloud import bigquery

    query_params = []
    for name, (bq_type, value) in (params or {}).items():
        query_params.append(bigquery.ScalarQueryParameter(name, bq_type, value))
    config = bigquery.QueryJobConfig(query_parameters=query_params)
    try:
        rows = bq_client().query(sql, job_config=config).result()
    except Exception as exc:
        logger.exception("BigQuery query failed")
        raise HTTPException(status_code=503, detail="Warehouse query failed") from exc
    return [dict(row) for row in rows]


# Endpoints
@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    bq_ok = pg_ok = False
    try:
        bq_client().query("SELECT 1").result()
        bq_ok = True
    except Exception:
        logger.warning("BigQuery health check failed", exc_info=True)
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        pg_ok = True
    except Exception:
        logger.warning("Postgres health check failed", exc_info=True)
    return HealthResponse(
        status="healthy" if bq_ok and pg_ok else "degraded",
        timestamp=dt.datetime.now(dt.timezone.utc),
        bigquery_connected=bq_ok,
        postgres_connected=pg_ok,
    )


def _best_prices_cte() -> str:
    """Current best price per product.

    For each product on each marketplace, take the listings seen within a day of that
    marketplace's latest scrape of the product (its current offers), then keep the
    cheapest across marketplaces. Products therefore always show a price, even when one
    marketplace was last scraped earlier than another; `last_seen` shows how fresh it is.
    """
    return f"""
        offers AS (
            SELECT f.product_key, f.effective_price, s.display_name, t.scraped_at,
                   MAX(t.scraped_at) OVER (PARTITION BY f.product_key, f.source_key) AS latest
            FROM {table("fact_price_snapshot")} f
            JOIN {table("dim_time")} t USING (time_key)
            JOIN {table("dim_source")} s USING (source_key)
        ),
        best AS (
            SELECT product_key,
                   MIN(effective_price) AS best_price,
                   ARRAY_AGG(display_name ORDER BY effective_price LIMIT 1)[OFFSET(0)] AS best_source,
                   MAX(scraped_at) AS last_seen
            FROM offers
            WHERE scraped_at >= TIMESTAMP_SUB(latest, INTERVAL 1 DAY)
            GROUP BY product_key
        )
    """


@app.get("/products", response_model=list[ProductSummary], dependencies=Protected)
def search_products(
    q: Annotated[str | None, Query(max_length=100, description="Words in the product name")] = None,
    category: str | None = None,
    cross_marketplace_only: bool = False,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> list[ProductSummary]:
    sql = f"""
        WITH {_best_prices_cte()}
        SELECT p.product_key, p.canonical_name, p.brand, p.category, p.model_number,
               p.source_count, p.is_cross_marketplace, b.best_price, b.best_source,
               b.last_seen
        FROM {table("dim_product")} p
        LEFT JOIN best b USING (product_key)
        WHERE (@q IS NULL OR LOWER(p.canonical_name) LIKE CONCAT('%', LOWER(@q), '%'))
          AND (@category IS NULL OR p.category = @category)
          AND (NOT @cross_only OR p.is_cross_marketplace)
        ORDER BY p.is_cross_marketplace DESC, p.source_count DESC, p.canonical_name
        LIMIT @limit OFFSET @offset
    """
    rows = run_query(
        sql,
        {
            "q": ("STRING", q),
            "category": ("STRING", category),
            "cross_only": ("BOOL", cross_marketplace_only),
            "limit": ("INT64", limit),
            "offset": ("INT64", offset),
        },
    )
    return [ProductSummary(**row) for row in rows]


@app.get("/products/{product_key}", response_model=ProductDetail, dependencies=Protected)
def get_product(product_key: str) -> ProductDetail:
    products = run_query(
        f"""
        SELECT product_key, canonical_name, brand, category, model_number,
               source_count, is_cross_marketplace
        FROM {table("dim_product")}
        WHERE product_key = @key
        """,
        {"key": ("STRING", product_key)},
    )
    if not products:
        raise HTTPException(status_code=404, detail="Product not found")

    # For each marketplace: the cheapest listing seen within a day of its latest scrape.
    offers = run_query(
        f"""
        WITH offers AS (
            SELECT s.marketplace_name AS marketplace, s.display_name, f.price,
                   f.effective_price, f.was_on_sale AS on_sale, f.listing_url,
                   t.scraped_at AS seen_at,
                   MAX(t.scraped_at) OVER (PARTITION BY f.source_key) AS latest
            FROM {table("fact_price_snapshot")} f
            JOIN {table("dim_source")} s USING (source_key)
            JOIN {table("dim_time")} t USING (time_key)
            WHERE f.product_key = @key
        )
        SELECT * EXCEPT (latest)
        FROM offers
        WHERE seen_at >= TIMESTAMP_SUB(latest, INTERVAL 1 DAY)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY marketplace ORDER BY effective_price, seen_at DESC) = 1
        ORDER BY effective_price
        """,
        {"key": ("STRING", product_key)},
    )
    offer_models = [Offer(**o) for o in offers]
    prices = [o.effective_price for o in offer_models]
    return ProductDetail(
        **products[0],
        best_price=min(prices) if prices else None,
        best_source=offer_models[0].display_name if offer_models else None,
        offers=offer_models,
        savings_vs_highest=round(max(prices) - min(prices), 2) if len(prices) > 1 else None,
    )


@app.get("/products/{product_key}/history", response_model=list[DailyPrice], dependencies=Protected)
def get_product_history(
    product_key: str, days: Annotated[int, Query(ge=1, le=365)] = 30
) -> list[DailyPrice]:
    rows = run_query(
        f"""
        SELECT t.date_day AS date, s.marketplace_name AS marketplace,
               MIN(f.effective_price) AS lowest_price
        FROM {table("fact_price_snapshot")} f
        JOIN {table("dim_time")} t USING (time_key)
        JOIN {table("dim_source")} s USING (source_key)
        WHERE f.product_key = @key
          AND t.date_day >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        GROUP BY date, marketplace
        ORDER BY date, marketplace
        """,
        {"key": ("STRING", product_key), "days": ("INT64", days)},
    )
    return [DailyPrice(**row) for row in rows]


@app.get("/price-history/{name}", response_model=PriceHistoryResponse, dependencies=Protected)
def get_price_history(
    name: str,
    days: Annotated[int, Query(ge=1, le=365)] = 30,
    limit: Annotated[int, Query(ge=1, le=1000)] = 200,
) -> PriceHistoryResponse:
    rows = run_query(
        f"""
        SELECT t.date_day AS date, p.canonical_name AS product, f.effective_price AS price,
               s.marketplace_name AS source, f.was_on_sale
        FROM {table("fact_price_snapshot")} f
        JOIN {table("dim_product")} p USING (product_key)
        JOIN {table("dim_source")} s USING (source_key)
        JOIN {table("dim_time")} t USING (time_key)
        WHERE LOWER(p.canonical_name) LIKE CONCAT('%', LOWER(@name), '%')
          AND t.date_day >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
        ORDER BY t.scraped_at DESC
        LIMIT @limit
        """,
        {"name": ("STRING", name), "days": ("INT64", days), "limit": ("INT64", limit)},
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No price history found for '{name}'")
    return PriceHistoryResponse(query=name, snapshots=[PriceSnapshot(**r) for r in rows])


@app.get("/anomalies", response_model=AnomaliesResponse, dependencies=Protected)
def get_anomalies(limit: Annotated[int, Query(ge=1, le=500)] = 50) -> AnomaliesResponse:
    """Anomalies flagged by ml/anomaly_detector.py; falls back to a z-score rule
    when the model has not been run yet."""
    from google.api_core.exceptions import NotFound

    try:
        rows = [
            dict(r)
            for r in bq_client()
            .query(
                f"""
                SELECT product, source, price, expected_price, drop_pct, DATE(scraped_at) AS date
                FROM {table("price_anomalies")}
                WHERE drop_pct > 0
                ORDER BY scraped_at DESC, drop_pct DESC
                LIMIT {int(limit)}
                """
            )
            .result()
        ]
        return AnomaliesResponse(method="isolation_forest", anomalies=[Anomaly(**r) for r in rows])
    except NotFound:
        logger.info("price_anomalies table not found; using z-score fallback")
    except Exception as exc:
        logger.exception("Reading price_anomalies failed")
        raise HTTPException(status_code=503, detail="Warehouse query failed") from exc

    rows = run_query(
        f"""
        WITH stats AS (
            SELECT product_key, source_key,
                   AVG(effective_price) AS avg_price, STDDEV(effective_price) AS std_price
            FROM {table("fact_price_snapshot")}
            GROUP BY product_key, source_key
        )
        SELECT p.canonical_name AS product, s.marketplace_name AS source,
               f.effective_price AS price, st.avg_price AS expected_price,
               ROUND((st.avg_price - f.effective_price) / st.avg_price * 100, 2) AS drop_pct,
               t.date_day AS date
        FROM {table("fact_price_snapshot")} f
        JOIN stats st USING (product_key, source_key)
        JOIN {table("dim_product")} p USING (product_key)
        JOIN {table("dim_source")} s USING (source_key)
        JOIN {table("dim_time")} t USING (time_key)
        WHERE st.std_price > 0 AND f.effective_price < st.avg_price - 2 * st.std_price
        ORDER BY t.scraped_at DESC, drop_pct DESC
        LIMIT @limit
        """,
        {"limit": ("INT64", limit)},
    )
    return AnomaliesResponse(method="zscore", anomalies=[Anomaly(**r) for r in rows])


@app.post("/track-product", response_model=TrackedProduct, status_code=201, dependencies=Protected)
def track_product(request: TrackProductRequest) -> TrackedProduct:
    with get_engine().begin() as conn:
        row = (
            conn.execute(
                text(
                    """
                INSERT INTO tracked_products (product_name, category)
                VALUES (:name, :category)
                ON CONFLICT (product_name, category)
                    DO UPDATE SET product_name = EXCLUDED.product_name
                RETURNING id, product_name, category, created_at
                """
                ),
                {"name": request.product_name.strip(), "category": request.category.strip()},
            )
            .mappings()
            .one()
        )
    return TrackedProduct(**row)


@app.get("/tracked-products", response_model=list[TrackedProduct], dependencies=Protected)
def list_tracked_products() -> list[TrackedProduct]:
    with get_engine().connect() as conn:
        rows = (
            conn.execute(
                text(
                    "SELECT id, product_name, category, created_at FROM tracked_products ORDER BY id"
                )
            )
            .mappings()
            .all()
        )
    return [TrackedProduct(**r) for r in rows]
