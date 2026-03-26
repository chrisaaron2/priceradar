"""
PriceRadar FastAPI Service
Owner: Punith (Track B)

Endpoints:
  GET  /health              - Health check
  POST /track-product       - Register a product for monitoring
  GET  /price-history/{name} - Last 30 days of price snapshots
  GET  /anomalies           - Recent price anomalies
"""

import os
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google.cloud import bigquery
import sqlalchemy as sa
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Config ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "priceradar-490621")
BQ_DATASET = "priceradar_warehouse"
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "priceradar")
POSTGRES_USER = os.getenv("POSTGRES_USER", "priceradar")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "priceradar_dev")
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# --- App ---
app = FastAPI(
    title="PriceRadar API",
    description="B2B Competitive Pricing Intelligence Platform — REST API for price tracking, history, and anomaly detection.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Pydantic Models ---
class TrackProductRequest(BaseModel):
    product_name: str
    category: str

class TrackProductResponse(BaseModel):
    status: str
    product_id: int

class PriceSnapshot(BaseModel):
    date: str
    price: float
    source: str
    was_on_sale: bool

class PriceHistoryResponse(BaseModel):
    product: str
    snapshots: list[PriceSnapshot]

class Anomaly(BaseModel):
    product: str
    price: float
    expected_price: float
    drop_pct: float
    date: str

class AnomaliesResponse(BaseModel):
    anomalies: list[Anomaly]

class HealthResponse(BaseModel):
    status: str
    timestamp: str
    bigquery_connected: bool
    postgres_connected: bool


# --- Helper Functions ---
def get_bq_client():
    return bigquery.Client(project=GCP_PROJECT_ID)


def get_pg_engine():
    return sa.create_engine(DATABASE_URL)


# --- Endpoints ---
@app.get("/health", response_model=HealthResponse)
def health_check():
    bq_ok = False
    pg_ok = False
    try:
        client = get_bq_client()
        client.query("SELECT 1").result()
        bq_ok = True
    except Exception:
        pass
    try:
        engine = get_pg_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        pg_ok = True
    except Exception:
        pass
    return HealthResponse(
        status="healthy" if (bq_ok and pg_ok) else "degraded",
        timestamp=datetime.now(timezone.utc).isoformat(),
        bigquery_connected=bq_ok,
        postgres_connected=pg_ok,
    )


@app.post("/track-product", response_model=TrackProductResponse)
def track_product(request: TrackProductRequest):
    try:
        engine = get_pg_engine()
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO raw_listings (product_name, category, source, price, scraped_at)
                    VALUES (:name, :category, 'tracked', 0, NOW())
                    RETURNING id
                """),
                {"name": request.product_name, "category": request.category},
            )
            product_id = result.scalar()
        return TrackProductResponse(status="tracking", product_id=product_id)
    except Exception as e:
        logger.error(f"Error tracking product: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/price-history/{canonical_name}", response_model=PriceHistoryResponse)
def get_price_history(canonical_name: str):
    try:
        client = get_bq_client()
        query = f"""
            SELECT
                DATE(t.scraped_at) AS date,
                f.price,
                s.marketplace_name AS source,
                f.was_on_sale
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_price_snapshot` f
            JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_product` p ON f.product_key = p.product_key
            JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_source` s ON f.source_key = s.source_key
            JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_time` t ON f.time_key = t.time_key
            WHERE LOWER(p.canonical_name) LIKE LOWER(@name)
            ORDER BY date DESC
            LIMIT 100
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("name", "STRING", f"%{canonical_name}%")
            ]
        )
        results = client.query(query, job_config=job_config).result()
        snapshots = [
            PriceSnapshot(
                date=str(row.date),
                price=float(row.price),
                source=row.source,
                was_on_sale=row.was_on_sale,
            )
            for row in results
        ]
        if not snapshots:
            raise HTTPException(status_code=404, detail=f"No price history found for '{canonical_name}'")
        return PriceHistoryResponse(product=canonical_name, snapshots=snapshots)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching price history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/anomalies", response_model=AnomaliesResponse)
def get_anomalies():
    try:
        client = get_bq_client()
        query = f"""
            WITH product_stats AS (
                SELECT
                    p.canonical_name,
                    AVG(f.price) AS avg_price,
                    STDDEV(f.price) AS std_price
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_price_snapshot` f
                JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_product` p ON f.product_key = p.product_key
                GROUP BY p.canonical_name
            ),
            flagged AS (
                SELECT
                    p.canonical_name AS product,
                    f.price,
                    ps.avg_price AS expected_price,
                    ROUND((ps.avg_price - f.price) / ps.avg_price * 100, 2) AS drop_pct,
                    DATE(t.scraped_at) AS date
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_price_snapshot` f
                JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_product` p ON f.product_key = p.product_key
                JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_time` t ON f.time_key = t.time_key
                JOIN product_stats ps ON p.canonical_name = ps.canonical_name
                WHERE f.price < ps.avg_price - (1.5 * ps.std_price)
                  AND ps.std_price > 0
            )
            SELECT product, price, expected_price, drop_pct, date
            FROM flagged
            ORDER BY drop_pct DESC
            LIMIT 50
        """
        results = client.query(query).result()
        anomalies = [
            Anomaly(
                product=row.product,
                price=float(row.price),
                expected_price=round(float(row.expected_price), 2),
                drop_pct=float(row.drop_pct),
                date=str(row.date),
            )
            for row in results
        ]
        return AnomaliesResponse(anomalies=anomalies)
    except Exception as e:
        logger.error(f"Error fetching anomalies: {e}")
        raise HTTPException(status_code=500, detail=str(e))