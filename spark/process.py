"""
PriceRadar - PySpark Processing Job

Reads raw listings from PostgreSQL, deduplicates, normalizes prices and
categories, adds computed columns, and writes clean Parquet to S3.

This is the Transform step: raw messy data → clean structured data.

Output Parquet matches the handoff contract:
  s3://priceradar-raw/processed/clean/date=YYYY-MM-DD/*.parquet

Owner: Chris (Track A)
"""

import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("spark_process")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POSTGRES_JDBC_URL = (
    f"jdbc:postgresql://"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'priceradar')}"
)
POSTGRES_USER = os.getenv("POSTGRES_USER", "priceradar")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "priceradar_dev")

S3_BUCKET = os.getenv("S3_BUCKET_NAME", "priceradar-raw")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Category normalization map
# Maps variations from both sources to unified category names
CATEGORY_MAP = {
    # Best Buy categories (already clean from our generator)
    "TVs": "TVs",
    "Laptops": "Laptops",
    "Headphones": "Headphones",
    "Smartwatches": "Smartwatches",
    "Tablets": "Tablets",
    # eBay category variations that might appear
    "Televisions": "TVs",
    "TV": "TVs",
    "Laptop": "Laptops",
    "Laptops & Netbooks": "Laptops",
    "Notebook": "Laptops",
    "Headphone": "Headphones",
    "Earbuds": "Headphones",
    "Smartwatch": "Smartwatches",
    "Smart Watches": "Smartwatches",
    "Tablet": "Tablets",
    "Tablets & eBook Readers": "Tablets",
    "iPad": "Tablets",
}

# Price bucket thresholds
BUDGET_MAX = 100.0
MID_MAX = 500.0


def create_spark_session() -> SparkSession:
    """
    Create a SparkSession configured for local mode with PostgreSQL JDBC
    and S3 (via Hadoop AWS) support.
    """
    # Download PostgreSQL JDBC driver if not present
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("priceradar-process")
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.7.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")  # Small dataset, fewer partitions
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created: %s", spark.version)
    return spark


def read_from_postgres(spark: SparkSession) -> DataFrame:
    """Read all raw_listings from PostgreSQL via JDBC."""
    logger.info("Reading from PostgreSQL: %s", POSTGRES_JDBC_URL)

    df = (
        spark.read
        .format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("dbtable", "raw_listings")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    row_count = df.count()
    logger.info("Read %d rows from raw_listings", row_count)
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplicate by (product_name, source, price), keeping the latest scraped_at.

    Why: If we run ingestion twice, the same product at the same price from
    the same source gets inserted twice. We keep only the most recent scrape.
    """
    window = Window.partitionBy("product_name", "source", "price").orderBy(
        F.col("scraped_at").desc()
    )

    df_deduped = (
        df
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    before = df.count()
    after = df_deduped.count()
    logger.info("Deduplication: %d → %d rows (removed %d duplicates)",
                before, after, before - after)
    return df_deduped


def normalize_prices(df: DataFrame) -> DataFrame:
    """
    Clean and normalize price fields.

    - Strip currency symbols (in case raw data has them)
    - Handle null sale_price
    - Ensure prices are positive doubles
    """
    df_clean = (
        df
        # Ensure price is a positive double
        .withColumn("price",
                     F.when(F.col("price").isNotNull() & (F.col("price") > 0),
                            F.col("price").cast("double"))
                     .otherwise(F.lit(None)))
        # Clean sale_price
        .withColumn("sale_price",
                     F.when(F.col("sale_price").isNotNull() & (F.col("sale_price") > 0),
                            F.col("sale_price").cast("double"))
                     .otherwise(F.lit(None)))
        # Ensure on_sale is consistent with sale_price
        .withColumn("on_sale",
                     F.when(F.col("sale_price").isNotNull() &
                            (F.col("sale_price") < F.col("price")),
                            F.lit(True))
                     .otherwise(F.lit(False)))
        # Drop rows with no valid price
        .filter(F.col("price").isNotNull())
    )

    logger.info("Price normalization complete: %d rows with valid prices", df_clean.count())
    return df_clean


def standardize_categories(df: DataFrame) -> DataFrame:
    """
    Map category variations to unified category names.

    Both eBay and Best Buy may use different names for the same category.
    This normalizes them to: TVs, Laptops, Headphones, Smartwatches, Tablets.
    """
    # Build a mapping expression using CASE WHEN
    mapping_expr = F.coalesce(
        *[
            F.when(F.col("category") == k, F.lit(v))
            for k, v in CATEGORY_MAP.items()
        ],
        F.col("category")  # Keep original if no match
    )

    df_mapped = df.withColumn("category", mapping_expr)

    # Show category distribution
    logger.info("Category distribution after standardization:")
    cat_counts = df_mapped.groupBy("source", "category").count().collect()
    for row in cat_counts:
        logger.info("  %s | %s: %d", row["source"], row["category"], row["count"])

    return df_mapped


def normalize_brands(df: DataFrame) -> DataFrame:
    """
    Normalize brand names: lowercase, strip common suffixes.

    'Samsung Electronics' and 'SAMSUNG' should both become 'samsung'.
    """
    df_clean = (
        df
        .withColumn("brand",
                     F.when(F.col("brand").isNotNull(),
                            F.lower(F.trim(F.col("brand"))))
                     .otherwise(F.lit("unknown")))
        # Strip common corporate suffixes
        .withColumn("brand",
                     F.regexp_replace(F.col("brand"),
                                      r"\s*(inc\.?|corp\.?|ltd\.?|co\.?|electronics|corporation)\s*$",
                                      ""))
        .withColumn("brand", F.trim(F.col("brand")))
    )

    logger.info("Brand normalization complete")
    return df_clean


def add_computed_columns(df: DataFrame) -> DataFrame:
    """
    Add derived columns:
    - price_bucket: "budget" (<$100), "mid" ($100-$500), "premium" (>$500)
    - Uses the effective price (sale_price if on sale, otherwise regular price)
    """
    # Effective price for bucketing
    effective_price = F.when(
        F.col("on_sale") & F.col("sale_price").isNotNull(),
        F.col("sale_price")
    ).otherwise(F.col("price"))

    df_enriched = (
        df
        .withColumn("price_bucket",
                     F.when(effective_price < BUDGET_MAX, F.lit("budget"))
                     .when(effective_price <= MID_MAX, F.lit("mid"))
                     .otherwise(F.lit("premium")))
    )

    # Log distribution
    bucket_counts = df_enriched.groupBy("price_bucket").count().collect()
    for row in bucket_counts:
        logger.info("  Price bucket '%s': %d products", row["price_bucket"], row["count"])

    return df_enriched


def select_output_columns(df: DataFrame) -> DataFrame:
    """
    Select and order columns to match the S3 Parquet handoff contract:

    product_name, price, sale_price, on_sale, source, url,
    category, brand, sku, scraped_at, price_bucket
    """
    return df.select(
        F.col("product_name").cast("string"),
        F.col("price").cast("double"),
        F.col("sale_price").cast("double"),
        F.col("on_sale").cast("boolean"),
        F.col("source").cast("string"),
        F.col("url").cast("string"),
        F.col("category").cast("string"),
        F.col("brand").cast("string"),
        F.col("sku").cast("string"),
        F.col("scraped_at").cast("timestamp"),
        F.col("price_bucket").cast("string"),
    )


def write_to_s3(df: DataFrame) -> str:
    """
    Write clean Parquet to S3 at:
    s3://priceradar-raw/processed/clean/date=YYYY-MM-DD/clean_listings.parquet

    Uses boto3 instead of Hadoop s3a connector to avoid Windows compatibility
    issues with Hadoop native libraries.
    """
    import io
    import boto3

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3_key = f"processed/clean/date={today}/clean_listings.parquet"

    logger.info("Writing %d rows to s3://%s/%s", df.count(), S3_BUCKET, s3_key)

    # Convert Spark DataFrame to Pandas, then to Parquet bytes
    pdf = df.toPandas()
    buffer = io.BytesIO()
    pdf.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Upload via boto3 (proven to work from Day 1)
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    output_path = f"s3://{S3_BUCKET}/{s3_key}"
    logger.info("Parquet write complete: %s", output_path)
    return output_path


def write_to_local(df: DataFrame, path: str = "output/clean_listings.parquet") -> str:
    """
    Fallback: write Parquet locally if S3 is unavailable.
    Uses pandas to avoid Hadoop native library issues on Windows.
    """
    import os as _os

    logger.info("Writing %d rows to local: %s", df.count(), path)

    _os.makedirs(_os.path.dirname(path) if _os.path.dirname(path) else "output", exist_ok=True)

    pdf = df.toPandas()
    pdf.to_parquet(path, index=False, engine="pyarrow")

    logger.info("Local Parquet write complete: %s", path)
    return path


def main() -> None:
    """
    Main processing pipeline:
    1. Read raw listings from PostgreSQL
    2. Deduplicate by (product_name, source, price)
    3. Normalize prices
    4. Standardize categories across sources
    5. Normalize brand names
    6. Add computed columns (price_bucket)
    7. Write clean Parquet to S3
    """
    logger.info("=" * 60)
    logger.info("Starting PriceRadar Spark processing")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        # Step 1: Read
        df_raw = read_from_postgres(spark)

        if df_raw.count() == 0:
            logger.warning("No data in raw_listings — nothing to process")
            return

        # Step 2: Deduplicate
        df_deduped = deduplicate(df_raw)

        # Step 3: Normalize prices
        df_prices = normalize_prices(df_deduped)

        # Step 4: Standardize categories
        df_categories = standardize_categories(df_prices)

        # Step 5: Normalize brands
        df_brands = normalize_brands(df_categories)

        # Step 6: Add computed columns
        df_enriched = add_computed_columns(df_brands)

        # Step 7: Select output columns matching handoff contract
        df_output = select_output_columns(df_enriched)

        # Step 8: Write to S3
        try:
            s3_path = write_to_s3(df_output)
            logger.info("S3 output: %s", s3_path)
        except Exception:
            logger.exception("S3 write failed — falling back to local output")
            local_path = write_to_local(df_output)
            logger.info("Local output: %s", local_path)

        # Summary
        logger.info("=" * 60)
        logger.info("Processing complete!")
        logger.info("  Input rows:  %d", df_raw.count())
        logger.info("  Output rows: %d", df_output.count())
        logger.info("  Sources: %s", [r["source"] for r in df_output.select("source").distinct().collect()])
        logger.info("  Categories: %s", [r["category"] for r in df_output.select("category").distinct().collect()])
        logger.info("=" * 60)

    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
