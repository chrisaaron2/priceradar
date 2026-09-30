"""
PriceRadar - next-price prediction (XGBoost), tracked in MLflow.

Predicts the next observed price of a product on a marketplace from its recent history
and calendar features. Trains on the older 80% of history and reports error on the
newest 20%, alongside a naive baseline (next price = current price) for comparison.
"""

from __future__ import annotations

import os

import mlflow
import mlflow.xgboost
import numpy as np
import xgboost as xgb
from dotenv import load_dotenv
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from common.log import get_logger
from ml.features import SERIES_KEYS, fetch_snapshots, time_split

load_dotenv()
logger = get_logger("price_predictor")

FEATURES = [
    "price",
    "hour",
    "day_of_week",
    "is_weekend",
    "week_number",
    "month",
    "rolling_mean_7d",
    "rolling_std_7d",
    "price_lag_1",
    "price_lag_3",
    "source_code",
    "category_code",
    "discount_pct",
]
PARAMS = {"learning_rate": 0.1, "max_depth": 6, "n_estimators": 200, "random_state": 42}


def main() -> None:
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment("priceradar-price-prediction")

    df = fetch_snapshots()
    df["target_price"] = df.groupby(SERIES_KEYS)["price"].shift(-1)
    df = df.dropna(subset=["target_price", "price_lag_1"])
    df[FEATURES] = df[FEATURES].astype(float).fillna(0.0)
    train, test = time_split(df)

    with mlflow.start_run(run_name="xgboost_next_price"):
        mlflow.log_params({**PARAMS, "features": ",".join(FEATURES)})
        mlflow.log_metrics({"n_train": len(train), "n_test": len(test)})

        model = xgb.XGBRegressor(**PARAMS).fit(train[FEATURES], train["target_price"])
        pred = model.predict(test[FEATURES])
        baseline = test["price"]
        metrics = {
            "rmse": float(np.sqrt(mean_squared_error(test["target_price"], pred))),
            "mae": float(mean_absolute_error(test["target_price"], pred)),
            "r_squared": float(r2_score(test["target_price"], pred)),
            "baseline_mae": float(mean_absolute_error(test["target_price"], baseline)),
        }
        mlflow.log_metrics(metrics)
        mlflow.xgboost.log_model(model, "price_predictor")
        logger.info("Holdout metrics: %s", metrics)


if __name__ == "__main__":
    main()
