FROM astrocrpublic.azurecr.io/runtime:3.1-14

# Install project Python dependencies
RUN pip install --no-cache-dir \
    boto3 \
    sqlalchemy \
    psycopg2-binary \
    requests \
    python-dotenv \
    pydantic \
    anthropic \
    pandas \
    pyarrow

# Copy project modules so DAG can import them
COPY ingestion/ /usr/local/airflow/ingestion/
COPY spark/ /usr/local/airflow/spark/
COPY llm/ /usr/local/airflow/llm/