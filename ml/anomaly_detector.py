"""
PriceRadar - price anomaly detection (IsolationForest), tracked in MLflow.

Flags observations whose price is unusual for that product on that marketplace. The
model trains on the older 80% of history and is evaluated on the newest 20% against a
simple rule (price more than 15% below the trailing 7-day median). Flagged drops are
written to BigQuery `price_anomalies`, which the API's /anomalies endpoint reads.
"""

from __future__ import annotations

import os

import mlflow
import mlflow.sklearn
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from sklearn.ensemble import IsolationForest
from sklearn.metrics import f1_score, precision_score, recall_score

from common.log import get_logger
from common.warehouse import bq_client, bq_dataset
from ml.features import fetch_snapshots, time_split

load_dotenv()
logger = get_logger("anomaly_detector")

FEATURES = [
    "price",
    "hour",
    "day_of_week",
    "is_weekend",
    "rolling_median_7d",
    "rolling_std_7d",
    "price_vs_median",
    "price_change_pct",
    "source_code",
    "category_code",
    "discount_pct",
]
RULE_DROP_PCT = 15
PARAMS = {"contamination": 0.05, "n_estimators": 200, "random_state": 42}


def rule_label(df: pd.DataFrame) -> pd.Series:
    return (df["price"] < df["rolling_median_7d"] * (1 - RULE_DROP_PCT / 100)).astype(int)


def main() -> None:
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment("priceradar-anomaly-detection")

    df = fetch_snapshots().dropna(subset=["rolling_median_7d"])
    df[FEATURES] = df[FEATURES].astype(float).fillna(0.0)
    train, test = time_split(df)

    with mlflow.start_run(run_name="isolation_forest"):
        mlflow.log_params(
            {**PARAMS, "rule_drop_pct": RULE_DROP_PCT, "features": ",".join(FEATURES)}
        )
        mlflow.log_metrics({"n_train": len(train), "n_test": len(test)})

        model = IsolationForest(**PARAMS).fit(train[FEATURES])
        y_true = rule_label(test)
        y_pred = (model.predict(test[FEATURES]) == -1).astype(int)
        metrics = {
            "test_precision": precision_score(y_true, y_pred, zero_division=0),
            "test_recall": recall_score(y_true, y_pred, zero_division=0),
            "test_f1": f1_score(y_true, y_pred, zero_division=0),
            "test_flagged": int(y_pred.sum()),
            "test_rule_positives": int(y_true.sum()),
        }
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(model, "anomaly_detector")
        logger.info("Holdout metrics: %s", metrics)

    # Score all history and keep only flagged price *drops*.
    df["is_anomaly"] = model.predict(df[FEATURES]) == -1
    flagged = df[df["is_anomaly"] & (df["price"] < df["rolling_median_7d"])].copy()
    flagged["expected_price"] = flagged["rolling_median_7d"].round(2)
    flagged["drop_pct"] = (
        (flagged["expected_price"] - flagged["price"]) / flagged["expected_price"] * 100
    ).round(2)
    flagged["detected_at"] = pd.Timestamp.now(tz="UTC")
    out = flagged[
        [
            "product_key",
            "product",
            "source",
            "category",
            "price",
            "expected_price",
            "drop_pct",
            "scraped_at",
            "detected_at",
        ]
    ]

    client = bq_client()
    table_id = f"{client.project}.{bq_dataset()}.price_anomalies"
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    client.load_table_from_dataframe(out, table_id, job_config=job_config).result()
    logger.info("Wrote %d anomalies to %s", len(out), table_id)


if __name__ == "__main__":
    main()
