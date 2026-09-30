"""Shared data access and feature engineering for the ML jobs."""

from __future__ import annotations

import pandas as pd

from common.log import get_logger
from common.warehouse import bq_client, table

logger = get_logger("ml.features")

SERIES_KEYS = ["product_key", "source"]


def fetch_snapshots() -> pd.DataFrame:
    """Every price observation with its product, marketplace, time and category."""
    sql = f"""
        SELECT f.product_key, p.canonical_name AS product, s.marketplace_name AS source,
               c.category_name AS category, f.effective_price AS price, f.was_on_sale,
               f.discount_pct, t.scraped_at, t.hour, t.day_of_week, t.is_weekend,
               t.week_number, t.month
        FROM {table("fact_price_snapshot")} f
        JOIN {table("dim_product")} p USING (product_key)
        JOIN {table("dim_source")} s USING (source_key)
        JOIN {table("dim_time")} t USING (time_key)
        LEFT JOIN {table("dim_category")} c USING (category_key)
    """
    df = bq_client().query(sql).to_dataframe()
    logger.info("Fetched %d price snapshots", len(df))
    return prepare(df)


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Sort by time and add per-series rolling features.

    A series is one product on one marketplace, so eBay and Best Buy prices for the
    same product are never mixed. Rolling windows cover the previous 7 *days*, not
    7 rows, and include only past observations (closed='left'), so no feature can see
    the price it describes.
    """
    df = df.copy()
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True)
    df = df.sort_values(SERIES_KEYS + ["scraped_at"]).reset_index(drop=True)

    def rolling(group: pd.DataFrame) -> pd.DataFrame:
        r = group.rolling("7D", on="scraped_at", closed="left")["price"]
        return pd.DataFrame(
            {
                "rolling_median_7d": r.median(),
                "rolling_mean_7d": r.mean(),
                "rolling_std_7d": r.std(),
            },
            index=group.index,
        )

    stats = df.groupby(SERIES_KEYS, group_keys=False)[["scraped_at", "price"]].apply(rolling)
    df = df.join(stats)

    grouped = df.groupby(SERIES_KEYS)["price"]
    df["price_lag_1"] = grouped.shift(1)
    df["price_lag_3"] = grouped.shift(3)
    df["price_change_pct"] = (df["price"] - df["price_lag_1"]) / df["price_lag_1"]
    df["price_vs_median"] = df["price"] / df["rolling_median_7d"]
    df["source_code"] = df["source"].astype("category").cat.codes
    df["category_code"] = df["category"].astype("category").cat.codes
    return df


def time_split(df: pd.DataFrame, test_fraction: float = 0.2) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split on time: everything before the cutoff trains, everything after tests."""
    cutoff = df["scraped_at"].quantile(1 - test_fraction)
    return df[df["scraped_at"] < cutoff], df[df["scraped_at"] >= cutoff]
