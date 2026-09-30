# Airflow image for `astro dev start`.
# Astro Runtime installs requirements.txt and packages.txt automatically.
FROM astrocrpublic.azurecr.io/runtime:3.1-14

# PySpark needs a Java runtime.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
USER astro

# dbt gets its own virtualenv so its dependencies never conflict with Airflow's.
RUN python -m venv /usr/local/airflow/dbt_venv \
    && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-bigquery>=1.10,<2"
ENV DBT_BIN=/usr/local/airflow/dbt_venv/bin/dbt

COPY common/ /usr/local/airflow/common/
COPY ingestion/ /usr/local/airflow/ingestion/
COPY spark/ /usr/local/airflow/spark/
COPY llm/ /usr/local/airflow/llm/
COPY dbt/ /usr/local/airflow/dbt/
