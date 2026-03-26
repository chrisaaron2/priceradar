"""
PriceRadar — Price Prediction using XGBoost
Owner: Punith (Track B)

Predicts next-day price for products based on time features
and historical patterns. Tracked in MLflow.
"""

import os
import logging
import warnings

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import xgboost as xgb
import mlflow
import mlflow.xgboost
from google.cloud import bigquery

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "priceradar-490621")
BQ_DATASET = "priceradar_warehouse"
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")


def fetch_data():
    """Fetch price data from BigQuery."""
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
            t.week_number,
            t.month,
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
    """Create features for price prediction."""
    df = df.copy()
    df["scraped_at"] = pd.to_datetime(df["scraped_at"])
    df = df.sort_values(["product", "scraped_at"])

    # Rolling features per product
    grouped = df.groupby("product")["price"]
    df["avg_price_7d"] = grouped.transform(lambda x: x.rolling(7, min_periods=1).mean())
    df["price_volatility_7d"] = grouped.transform(lambda x: x.rolling(7, min_periods=1).std().fillna(0))
    df["price_lag_1"] = grouped.shift(1)
    df["price_lag_3"] = grouped.shift(3)

    # Days since last sale per product
    df["days_since_sale"] = df.groupby("product")["was_on_sale"].transform(
        lambda x: x.cumsum().groupby(x.cumsum()).cumcount()
    )

    # Encode categoricals
    le_source = LabelEncoder()
    le_category = LabelEncoder()
    df["source_encoded"] = le_source.fit_transform(df["source"])
    df["category_encoded"] = le_category.fit_transform(df["category"])

    # Target: next price (shift -1)
    df["target_price"] = grouped.shift(-1)

    # Drop rows with no target
    df = df.dropna(subset=["target_price", "price_lag_1"])

    feature_cols = [
        "price", "hour", "day_of_week", "is_weekend", "week_number", "month",
        "avg_price_7d", "price_volatility_7d", "price_lag_1", "price_lag_3",
        "days_since_sale", "source_encoded", "category_encoded", "discount_pct"
    ]

    df[feature_cols] = df[feature_cols].fillna(0)
    return df, feature_cols


def train_and_log(df, feature_cols):
    """Train XGBoost model and log to MLflow."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("priceradar-price-prediction")

    # Parameters
    learning_rate = 0.1
    max_depth = 6
    n_estimators = 100

    X = df[feature_cols].values
    y = df["target_price"].values

    # Time-based split: 80% train, 20% test
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    with mlflow.start_run(run_name="xgboost_price_v1"):
        # Log parameters
        mlflow.log_param("learning_rate", learning_rate)
        mlflow.log_param("max_depth", max_depth)
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("n_features", len(feature_cols))
        mlflow.log_param("n_train", len(X_train))
        mlflow.log_param("n_test", len(X_test))
        mlflow.log_param("features", ", ".join(feature_cols))

        # Train model
        model = xgb.XGBRegressor(
            learning_rate=learning_rate,
            max_depth=max_depth,
            n_estimators=n_estimators,
            random_state=42,
        )
        model.fit(X_train, y_train)

        # Predict
        y_pred = model.predict(X_test)

        # Metrics
        rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
        mae = float(mean_absolute_error(y_test, y_pred))
        r2 = float(r2_score(y_test, y_pred))

        mlflow.log_metric("rmse", round(rmse, 4))
        mlflow.log_metric("mae", round(mae, 4))
        mlflow.log_metric("r_squared", round(r2, 4))

        # Log model
        mlflow.xgboost.log_model(model, "price_predictor")

        logger.info(f"RMSE: {rmse:.4f}")
        logger.info(f"MAE: {mae:.4f}")
        logger.info(f"R-squared: {r2:.4f}")

    return model


def main():
    logger.info("=" * 60)
    logger.info("PriceRadar Price Prediction — Starting")
    logger.info("=" * 60)

    df = fetch_data()
    df, feature_cols = engineer_features(df)
    model = train_and_log(df, feature_cols)

    logger.info("=" * 60)
    logger.info("Price Prediction — Complete")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()