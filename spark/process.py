"""
PriceRadar - PySpark Processing Job

Reads raw listings from PostgreSQL, deduplicates, normalizes prices and
categories, adds computed columns, and writes clean Parquet to S3.

This is the Transform step: raw messy data → clean structured data.

Output Parquet matches the handoff contract:
  s3://priceradar-raw/processed/clean/date=YYYY-MM-DD/*.parquet

"""

import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv

from common.log import get_logger

load_dotenv()

logger = get_logger("spark_process")

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
    Create a local-mode SparkSession with the PostgreSQL JDBC driver.
    (S3 writes go through boto3, so no Hadoop AWS jars are needed.)
    """
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("priceradar-process")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
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
        spark.read.format("jdbc")
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
    """Drop repeated scrapes of the same listing within the same hour.

    Two rows count as duplicates when they have the same source, title, price and sale
    price and were scraped in the same hour (for example, a task retried by Airflow).
    Price changes over time are kept, so the price history survives cleaning.
    """
    window = Window.partitionBy(
        "source",
        "product_name",
        "price",
        "sale_price",
        F.date_trunc("hour", F.col("scraped_at")),
    ).orderBy(F.col("scraped_at").desc(), F.col("id").desc())

    df_deduped = (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    before, after = df.count(), df_deduped.count()
    logger.info("Deduplication: %d -> %d rows (removed %d)", before, after, before - after)
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
        .withColumn(
            "price",
            F.when(
                F.col("price").isNotNull() & (F.col("price") > 0), F.col("price").cast("double")
            ).otherwise(F.lit(None)),
        )
        # Clean sale_price
        .withColumn(
            "sale_price",
            F.when(
                F.col("sale_price").isNotNull() & (F.col("sale_price") > 0),
                F.col("sale_price").cast("double"),
            ).otherwise(F.lit(None)),
        )
        # Ensure on_sale is consistent with sale_price
        .withColumn(
            "on_sale",
            F.when(
                F.col("sale_price").isNotNull() & (F.col("sale_price") < F.col("price")),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
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
        *[F.when(F.col("category") == k, F.lit(v)) for k, v in CATEGORY_MAP.items()],
        F.col("category"),  # Keep original if no match
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
        df.withColumn(
            "brand",
            F.when(F.col("brand").isNotNull(), F.lower(F.trim(F.col("brand")))).otherwise(
                F.lit("unknown")
            ),
        )
        # Strip common corporate suffixes
        .withColumn(
            "brand",
            F.regexp_replace(
                F.col("brand"), r"\s*(inc\.?|corp\.?|ltd\.?|co\.?|electronics|corporation)\s*$", ""
            ),
        )
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
        F.col("on_sale") & F.col("sale_price").isNotNull(), F.col("sale_price")
    ).otherwise(F.col("price"))

    df_enriched = df.withColumn(
        "price_bucket",
        F.when(effective_price < BUDGET_MAX, F.lit("budget"))
        .when(effective_price <= MID_MAX, F.lit("mid"))
        .otherwise(F.lit("premium")),
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
    """Write clean Parquet to s3://<bucket>/processed/clean/date=YYYY-MM-DD/clean_listings.parquet.

    Converts to pandas and uploads with boto3 rather than the Hadoop s3a connector,
    which avoids Hadoop native-library problems on Windows. Fine at this data size.
    """
    import io

    from common.storage import upload_bytes

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"processed/clean/date={today}/clean_listings.parquet"

    buffer = io.BytesIO()
    df.toPandas().to_parquet(buffer, index=False, engine="pyarrow")
    uri = upload_bytes(buffer.getvalue(), key)
    logger.info("Parquet written to %s", uri)
    return uri


def write_to_local(df: DataFrame, root: str = "output/clean") -> str:
    """Write clean Parquet locally when S3 is not configured or unavailable.

    Mirrors the S3 layout: output/clean/date=YYYY-MM-DD/clean_listings.parquet, so each
    day's snapshot is kept and a rerun on the same day replaces that day's file.
    Uses pandas to avoid Hadoop native-library issues on Windows.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    folder = Path(root) / f"date={today}"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "clean_listings.parquet"
    df.toPandas().to_parquet(path, index=False, engine="pyarrow")
    logger.info("Parquet written to %s", path)
    return str(path)


def main() -> None:
    """
    Main processing pipeline:
    1. Read raw listings from PostgreSQL
    2. Drop repeated scrapes of the same listing within an hour
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

        # Step 8: Write Parquet to S3, or locally when no bucket is configured
        from common.storage import bucket_name

        if bucket_name():
            try:
                logger.info("S3 output: %s", write_to_s3(df_output))
            except Exception:
                logger.exception("S3 write failed; falling back to local output")
                logger.info("Local output: %s", write_to_local(df_output))
        else:
            logger.info("S3_BUCKET_NAME not set; writing Parquet locally")
            logger.info("Local output: %s", write_to_local(df_output))

        # Summary
        logger.info("=" * 60)
        logger.info("Processing complete!")
        logger.info("  Input rows:  %d", df_raw.count())
        logger.info("  Output rows: %d", df_output.count())
        logger.info(
            "  Sources: %s", [r["source"] for r in df_output.select("source").distinct().collect()]
        )
        logger.info(
            "  Categories: %s",
            [r["category"] for r in df_output.select("category").distinct().collect()],
        )
        logger.info("=" * 60)

    finally:
        spark.stop()
        logger.info("SparkSession stopped")


if __name__ == "__main__":
    main()
