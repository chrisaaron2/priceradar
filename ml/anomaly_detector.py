"""
PriceRadar — Anomaly Detection using IsolationForest
Owner: Punith (Track B)

Detects price anomalies where a product's price drops significantly
below its rolling average. Tracked in MLflow.
"""

import os
import logging
import warnings

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.metrics import precision_score, recall_score, f1_score
import mlflow
import mlflow.sklearn
from google.cloud import bigquery

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "priceradar-490621")
BQ_DATASET = "priceradar_warehouse"
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")


def fetch_data():
    """Fetch price snapshot data from BigQuery."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT
            p.canonical_name AS product,
            f.price,
            f.was_on_sale,
            f.discount_pct,
            t.scraped_at,
            t.hour,
            t.day_of_week,
            t.is_weekend,
            s.marketplace_name AS source,
            c.category_name AS category
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_price_snapshot` f
        JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_product` p ON f.product_key = p.product_key
        JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_time` t ON f.time_key = t.time_key
        JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_source` s ON f.source_key = s.source_key
        JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.dim_category` c ON f.category_key = c.category_key
        ORDER BY t.scraped_at
    """
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} rows from BigQuery")
    return df


def engineer_features(df):
    """Create features for anomaly detection."""
    df = df.copy()
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    df = df.sort_values(["product", "scraped_at"])

    # Rolling statistics per product
    grouped = df.groupby("product")["price"]
    df["rolling_median_7d"] = grouped.transform(lambda x: x.rolling(7, min_periods=1).median())
    df["rolling_std_7d"] = grouped.transform(lambda x: x.rolling(7, min_periods=1).std().fillna(0))
    df["price_vs_median_ratio"] = df["price"] / df["rolling_median_7d"].replace(0, np.nan)
    df["price_vs_median_ratio"] = df["price_vs_median_ratio"].fillna(1.0)

    # Price change from previous observation
    df["prev_price"] = grouped.shift(1)
    df["price_change_pct"] = ((df["price"] - df["prev_price"]) / df["prev_price"].replace(0, np.nan)).fillna(0)

    # Encode categoricals
    df["source_encoded"] = (df["source"] == "bestbuy").astype(int)
    df["category_encoded"] = pd.Categorical(df["category"]).codes

    # Select features
    feature_cols = [
        "price", "hour", "day_of_week", "is_weekend",
        "rolling_median_7d", "rolling_std_7d",
        "price_vs_median_ratio", "price_change_pct",
        "source_encoded", "category_encoded", "discount_pct"
    ]

    df[feature_cols] = df[feature_cols].fillna(0)
    return df, feature_cols


def create_labels(df, threshold_pct=15):
    """Create ground truth labels: anomaly if price drops >threshold% below rolling median."""
    df["is_anomaly_true"] = (
        (df["rolling_median_7d"] > 0) &
        (df["price"] < df["rolling_median_7d"] * (1 - threshold_pct / 100))
    ).astype(int)
    num_anomalies = df["is_anomaly_true"].sum()
    logger.info(f"Ground truth anomalies (>{threshold_pct}% below median): {num_anomalies} / {len(df)}")
    return df


def train_and_log(df, feature_cols):
    """Train IsolationForest and log to MLflow."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("priceradar-anomaly-detection")

    # Model parameters
    contamination = 0.05
    n_estimators = 100
    threshold_pct = 15
    random_state = 42

    X = df[feature_cols].values
    y_true = df["is_anomaly_true"].values

    with mlflow.start_run(run_name="isolation_forest_v1"):
        # Log parameters
        mlflow.log_param("contamination", contamination)
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("threshold_pct", threshold_pct)
        mlflow.log_param("random_state", random_state)
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("n_samples", len(df))
        mlflow.log_param("features", ", ".join(feature_cols))

        # Train model
        model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=random_state,
        )
        model.fit(X)

        # Predict: IsolationForest returns -1 for anomaly, 1 for normal
        y_pred_raw = model.predict(X)
        y_pred = (y_pred_raw == -1).astype(int)

        # Metrics
        num_detected = y_pred.sum()
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)

        mlflow.log_metric("precision", round(precision, 4))
        mlflow.log_metric("recall", round(recall, 4))
        mlflow.log_metric("f1", round(f1, 4))
        mlflow.log_metric("num_anomalies_detected", num_detected)
        mlflow.log_metric("num_anomalies_true", int(y_true.sum()))

        # Log model
        mlflow.sklearn.log_model(model, "anomaly_detector")

        logger.info(f"Anomalies detected: {num_detected}")
        logger.info(f"Precision: {precision:.4f}, Recall: {recall:.4f}, F1: {f1:.4f}")

    # Add predictions to dataframe
    df["is_anomaly_predicted"] = y_pred
    return df, model


def save_anomalies_to_bigquery(df):
    """Save detected anomalies to BigQuery."""
    anomalies = df[df["is_anomaly_predicted"] == 1][
        ["product", "price", "rolling_median_7d", "scraped_at", "source", "category"]
    ].copy()
    anomalies = anomalies.rename(columns={"rolling_median_7d": "expected_price"})
    anomalies["drop_pct"] = round(
        (anomalies["expected_price"] - anomalies["price"]) / anomalies["expected_price"] * 100, 2
    )
    anomalies["detected_at"] = pd.Timestamp.now(tz="UTC")

    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.price_anomalies"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(anomalies, table_id, job_config=job_config)
    job.result()
    logger.info(f"Saved {len(anomalies)} anomalies to BigQuery: {table_id}")


def main():
    logger.info("=" * 60)
    logger.info("PriceRadar Anomaly Detection — Starting")
    logger.info("=" * 60)

    df = fetch_data()
    df, feature_cols = engineer_features(df)
    df = create_labels(df, threshold_pct=15)
    df, model = train_and_log(df, feature_cols)
    save_anomalies_to_bigquery(df)

    logger.info("=" * 60)
    logger.info("Anomaly Detection — Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()