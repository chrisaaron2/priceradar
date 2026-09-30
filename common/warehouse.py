"""BigQuery settings shared by the loader, the API, the ML jobs and the DAG."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

# Drop your service-account key here (git-ignored). Scripts on your machine, the
# Airflow containers and the API container all find it at this path inside the repo.
DEFAULT_KEY_FILE = Path(__file__).resolve().parent.parent / "include" / "gcp-key.json"


def credentials_path() -> str | None:
    """Service-account key to use: GOOGLE_APPLICATION_CREDENTIALS if it points at a real
    file, otherwise include/gcp-key.json if present, otherwise None."""
    configured = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if configured and Path(configured).is_file():
        return configured
    if DEFAULT_KEY_FILE.is_file():
        return str(DEFAULT_KEY_FILE)
    return None


def use_credentials() -> str | None:
    """Point GOOGLE_APPLICATION_CREDENTIALS at the resolved key so every Google client
    (and dbt) picks it up. Returns the path, or None when no key was found."""
    path = credentials_path()
    if path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
    return path


def gcp_project() -> str | None:
    """GCP project ID; None lets the client infer it from the credentials."""
    return os.getenv("GCP_PROJECT_ID") or None


def bq_dataset() -> str:
    return os.getenv("BQ_DATASET", "priceradar_warehouse")


@lru_cache(maxsize=1)
def bq_client():
    from google.cloud import bigquery

    use_credentials()
    return bigquery.Client(project=gcp_project())


def table(name: str) -> str:
    """Fully qualified, backtick-quoted table name for use in SQL."""
    return f"`{bq_client().project}.{bq_dataset()}.{name}`"
