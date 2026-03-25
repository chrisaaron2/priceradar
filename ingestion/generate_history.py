"""
PriceRadar - Synthetic Historical Data Generator

Generates 30 days of realistic price history for all products in our catalog.
This gives Punith's ML models (anomaly detection, price prediction) enough
training data, and makes dashboards show meaningful trends.

Simulates:
- Daily price snapshots (4x per day to match 6-hour ingestion schedule)
- Natural price fluctuations (±2-5% daily variance)
- Periodic sales (weekly patterns, weekend sales)
- Occasional price drops (simulated promotions)
- Gradual price trends (some products trending up/down over 30 days)

Owner: Chris (Track A)
"""

import json
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import boto3
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("history_generator")

# Same product catalog as bestbuy_ingest.py
PRODUCT_CATALOG: dict[str, list[dict[str, Any]]] = {
    "TVs": [
        {"name": "Samsung - 65\" Class QN85D Neo QLED 4K Smart TV", "brand": "Samsung", "model": "QN65QN85D", "base_price": 1199.99},
        {"name": "LG - 65\" Class C4 Series OLED 4K UHD Smart TV", "brand": "LG", "model": "OLED65C4PUA", "base_price": 1599.99},
        {"name": "Sony - 65\" Class BRAVIA XR A95L QD-OLED 4K Smart TV", "brand": "Sony", "model": "XR65A95L", "base_price": 2599.99},
        {"name": "TCL - 65\" Class Q7 Q-Series QLED 4K Smart TV", "brand": "TCL", "model": "65Q750G", "base_price": 599.99},
        {"name": "Hisense - 65\" Class U8N Mini-LED 4K Smart TV", "brand": "Hisense", "model": "65U8N", "base_price": 999.99},
    ],
    "Laptops": [
        {"name": "Apple MacBook Air 15\" Laptop - M3 chip - 16GB Memory - 256GB SSD", "brand": "Apple", "model": "MRYQ3LL/A", "base_price": 1199.99},
        {"name": "Dell - XPS 14 14\" OLED Touch Laptop - Intel Core Ultra 7 - 16GB Memory - 512GB SSD", "brand": "Dell", "model": "XPS9440", "base_price": 1499.99},
        {"name": "HP - OMEN 16.1\" Gaming Laptop - Intel Core i9 - 32GB Memory - NVIDIA RTX 4070 - 1TB SSD", "brand": "HP", "model": "16-wf1075cl", "base_price": 1599.99},
        {"name": "Lenovo - ThinkPad X1 Carbon Gen 12 14\" Touch Laptop - Intel Core Ultra 7", "brand": "Lenovo", "model": "21KC005YUS", "base_price": 1499.99},
        {"name": "ASUS - ROG Strix G16 16\" Gaming Laptop - Intel Core i9 - NVIDIA RTX 4060", "brand": "ASUS", "model": "G614JU-ES94", "base_price": 1299.99},
    ],
    "Headphones": [
        {"name": "Sony - WH-1000XM5 Wireless Noise Cancelling Over-Ear Headphones", "brand": "Sony", "model": "WH1000XM5/B", "base_price": 349.99},
        {"name": "Apple - AirPods Pro 2nd Generation with USB-C", "brand": "Apple", "model": "MTJV3AM/A", "base_price": 219.99},
        {"name": "Apple - AirPods Max - USB-C", "brand": "Apple", "model": "MUW63AM/A", "base_price": 499.99},
        {"name": "Bose - QuietComfort Ultra Wireless Noise Cancelling Over-Ear Headphones", "brand": "Bose", "model": "880066-0100", "base_price": 389.99},
        {"name": "Samsung - Galaxy Buds3 Pro True Wireless Noise Cancelling Earbuds", "brand": "Samsung", "model": "SM-R630NZAAXAR", "base_price": 229.99},
    ],
    "Smartwatches": [
        {"name": "Apple Watch Series 10 GPS 46mm Aluminum Case with Sport Band", "brand": "Apple", "model": "MXM23LL/A", "base_price": 399.99},
        {"name": "Samsung - Galaxy Watch7 Smartwatch 44mm BT Aluminum Case", "brand": "Samsung", "model": "SM-L505DZGAXAA", "base_price": 299.99},
        {"name": "Google - Pixel Watch 3 45mm Smartwatch with Obsidian Active Band", "brand": "Google", "model": "GA05764-US", "base_price": 379.99},
        {"name": "Garmin - Venu 3 GPS Smartwatch 45mm AMOLED", "brand": "Garmin", "model": "010-02784-01", "base_price": 429.99},
    ],
    "Tablets": [
        {"name": "Apple - iPad Pro 11\" M4 chip Wi-Fi 256GB", "brand": "Apple", "model": "MW5H3LL/A", "base_price": 1049.99},
        {"name": "Samsung - Galaxy Tab S10 Ultra 14.6\" 256GB Wi-Fi", "brand": "Samsung", "model": "SM-X820NZAAXAR", "base_price": 1149.99},
        {"name": "Apple - iPad Air 13\" M2 chip Wi-Fi 128GB", "brand": "Apple", "model": "MV273LL/A", "base_price": 779.99},
        {"name": "Microsoft - Surface Pro 11th Edition 13\" Snapdragon X Plus", "brand": "Microsoft", "model": "ZHY-00001", "base_price": 1049.99},
    ],
}

# eBay versions of the same products (slightly different naming)
EBAY_VARIANTS: dict[str, list[dict[str, Any]]] = {
    "TVs": [
        {"name": "Samsung 65\" QN85D Neo QLED 4K Smart TV QN65QN85D NEW", "brand": "Samsung", "model": "QN65QN85D", "base_price": 1149.99},
        {"name": "LG 65\" C4 OLED evo 4K Smart TV OLED65C4PUA", "brand": "LG", "model": "OLED65C4PUA", "base_price": 1549.99},
        {"name": "Sony 65\" BRAVIA XR A95L QD-OLED 4K HDR Smart TV XR65A95L", "brand": "Sony", "model": "XR65A95L", "base_price": 2499.99},
    ],
    "Laptops": [
        {"name": "Apple MacBook Air 15 M3 Chip 16GB 256GB SSD - New Sealed", "brand": "Apple", "model": "MRYQ3LL/A", "base_price": 1149.99},
        {"name": "Dell XPS 14 OLED Intel Core Ultra 7 16GB 512GB Laptop NEW", "brand": "Dell", "model": "XPS9440", "base_price": 1449.99},
        {"name": "ASUS ROG Strix G16 Gaming Laptop i9 RTX 4060 16GB 1TB", "brand": "ASUS", "model": "G614JU-ES94", "base_price": 1249.99},
    ],
    "Headphones": [
        {"name": "Sony WH-1000XM5 Wireless Noise Canceling Headphones Black NEW", "brand": "Sony", "model": "WH1000XM5/B", "base_price": 299.99},
        {"name": "Apple AirPods Pro 2nd Gen USB-C MagSafe - New Sealed", "brand": "Apple", "model": "MTJV3AM/A", "base_price": 189.99},
        {"name": "Bose QuietComfort Ultra Over-Ear Wireless Headphones Black", "brand": "Bose", "model": "880066-0100", "base_price": 349.99},
        {"name": "Samsung Galaxy Buds3 Pro Wireless ANC Earbuds Silver", "brand": "Samsung", "model": "SM-R630NZAAXAR", "base_price": 199.99},
    ],
    "Smartwatches": [
        {"name": "Apple Watch Series 10 GPS 46mm Aluminum Silver Sport Band NEW", "brand": "Apple", "model": "MXM23LL/A", "base_price": 379.99},
        {"name": "Samsung Galaxy Watch 7 44mm Bluetooth Smartwatch Green", "brand": "Samsung", "model": "SM-L505DZGAXAA", "base_price": 269.99},
        {"name": "Google Pixel Watch 3 45mm GPS WiFi Obsidian Band", "brand": "Google", "model": "GA05764-US", "base_price": 349.99},
    ],
    "Tablets": [
        {"name": "Apple iPad Pro 11 M4 WiFi 256GB Space Black NEW Sealed", "brand": "Apple", "model": "MW5H3LL/A", "base_price": 999.99},
        {"name": "Samsung Galaxy Tab S10 Ultra 14.6\" 256GB WiFi Tablet NEW", "brand": "Samsung", "model": "SM-X820NZAAXAR", "base_price": 1099.99},
    ],
}


def generate_price_history(
    base_price: float,
    days: int = 30,
    snapshots_per_day: int = 4,
) -> list[dict[str, float | bool]]:
    """
    Generate realistic price history for a product over N days.

    Simulates:
    - Daily random walk (±1-3% variance)
    - Weekend sale probability (higher on Fri-Sun)
    - Occasional deep discounts (5-20% off, ~10% chance per day)
    - Mean reversion (prices tend to drift back toward base_price)
    """
    history = []
    current_price = base_price
    trend = random.uniform(-0.001, 0.001)  # Slight up/down trend per product

    for day in range(days, 0, -1):
        timestamp = datetime.now(timezone.utc) - timedelta(days=day)
        day_of_week = timestamp.weekday()  # 0=Mon, 6=Sun
        is_weekend = day_of_week >= 5

        # Daily price movement (random walk with mean reversion)
        daily_change = random.gauss(0, 0.015)  # ~1.5% daily volatility
        mean_reversion = (base_price - current_price) / base_price * 0.1
        current_price *= (1 + daily_change + mean_reversion + trend)

        # Ensure price stays within realistic bounds (±30% of base)
        current_price = max(base_price * 0.70, min(base_price * 1.30, current_price))

        # Sale logic
        sale_chance = 0.15 if is_weekend else 0.08  # Higher on weekends
        on_sale = random.random() < sale_chance

        if on_sale:
            discount = random.uniform(0.05, 0.20)  # 5-20% off
            sale_price = round(current_price * (1 - discount), 2)
        else:
            sale_price = None

        # Generate snapshots for each 6-hour window
        for snapshot in range(snapshots_per_day):
            snap_time = timestamp + timedelta(hours=snapshot * 6)
            # Tiny variation between snapshots in same day
            snap_price = round(current_price * random.uniform(0.998, 1.002), 2)

            history.append({
                "price": snap_price,
                "sale_price": sale_price,
                "on_sale": on_sale,
                "scraped_at": snap_time,
            })

    return history


def generate_sku() -> str:
    """Generate a realistic SKU."""
    return str(random.randint(6000000, 9999999))


def generate_all_history(days: int = 30) -> list[dict[str, Any]]:
    """Generate historical listings for all products from both sources."""
    all_listings = []

    # Best Buy products
    for category, products in PRODUCT_CATALOG.items():
        for product in products:
            history = generate_price_history(product["base_price"], days=days)
            for snapshot in history:
                listing = {
                    "product_name": product["name"],
                    "price": snapshot["price"],
                    "sale_price": snapshot["sale_price"],
                    "on_sale": snapshot["on_sale"],
                    "source": "bestbuy",
                    "url": f"https://www.bestbuy.com/site/{generate_sku()}.p",
                    "category": category,
                    "brand": product["brand"],
                    "sku": generate_sku(),
                    "scraped_at": snapshot["scraped_at"],
                    "raw_payload": {
                        "name": product["name"],
                        "modelNumber": product["model"],
                        "manufacturer": product["brand"],
                        "_synthetic": True,
                        "_historical": True,
                    },
                }
                all_listings.append(listing)

    # eBay products (slightly different prices)
    for category, products in EBAY_VARIANTS.items():
        for product in products:
            history = generate_price_history(product["base_price"], days=days)
            for snapshot in history:
                listing = {
                    "product_name": product["name"],
                    "price": snapshot["price"],
                    "sale_price": snapshot["sale_price"],
                    "on_sale": snapshot["on_sale"],
                    "source": "ebay",
                    "url": f"https://www.ebay.com/itm/{random.randint(100000000000, 399999999999)}",
                    "category": category,
                    "brand": product["brand"],
                    "sku": f"v1|{random.randint(100000000000, 399999999999)}|0",
                    "scraped_at": snapshot["scraped_at"],
                    "raw_payload": {
                        "title": product["name"],
                        "itemId": f"v1|{random.randint(100000000000, 399999999999)}|0",
                        "_synthetic": True,
                        "_historical": True,
                    },
                }
                all_listings.append(listing)

    logger.info("Generated %d historical listings (%d Best Buy + %d eBay) over %d days",
                len(all_listings),
                sum(1 for l in all_listings if l["source"] == "bestbuy"),
                sum(1 for l in all_listings if l["source"] == "ebay"),
                days)
    return all_listings


def save_to_postgres(listings: list[dict[str, Any]]) -> int:
    """Insert historical listings into PostgreSQL raw_listings table."""
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "priceradar")
    pg_user = os.getenv("POSTGRES_USER", "priceradar")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "priceradar_dev")

    connection_string = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(connection_string)
    inserted = 0

    try:
        # Insert in batches for performance
        batch_size = 500
        with engine.begin() as conn:
            for i in range(0, len(listings), batch_size):
                batch = listings[i:i + batch_size]
                for listing in batch:
                    conn.execute(
                        text("""
                            INSERT INTO raw_listings
                                (product_name, price, sale_price, on_sale, source,
                                 url, category, brand, sku, scraped_at, raw_payload)
                            VALUES
                                (:product_name, :price, :sale_price, :on_sale, :source,
                                 :url, :category, :brand, :sku, :scraped_at,
                                 CAST(:raw_payload AS jsonb))
                        """),
                        {
                            "product_name": listing["product_name"],
                            "price": listing["price"],
                            "sale_price": listing["sale_price"],
                            "on_sale": listing["on_sale"],
                            "source": listing["source"],
                            "url": listing["url"],
                            "category": listing["category"],
                            "brand": listing["brand"],
                            "sku": listing["sku"],
                            "scraped_at": listing["scraped_at"],
                            "raw_payload": json.dumps(listing["raw_payload"], default=str),
                        },
                    )
                    inserted += 1

                logger.info("Inserted batch %d-%d (%d/%d)",
                             i, i + len(batch), inserted, len(listings))

        logger.info("Total inserted: %d rows", inserted)
        return inserted

    except Exception:
        logger.exception("Failed to insert into PostgreSQL")
        return inserted


def save_to_s3(listings: list[dict[str, Any]]) -> str | None:
    """Save historical data to S3."""
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        logger.warning("S3_BUCKET_NAME not set — skipping S3 upload")
        return None

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    key = f"raw/historical/{timestamp}.json"

    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(listings, indent=2, default=str),
            ContentType="application/json",
        )
        logger.info("Uploaded to s3://%s/%s", bucket, key)
        return f"s3://{bucket}/{key}"
    except Exception:
        logger.exception("Failed to upload to S3")
        return None


def main(days: int = 30) -> None:
    """Generate and load synthetic historical price data."""
    logger.info("=" * 60)
    logger.info("Generating %d days of synthetic price history", days)
    logger.info("=" * 60)

    listings = generate_all_history(days=days)

    # Save to Postgres
    count = save_to_postgres(listings)
    logger.info("PostgreSQL: %d rows inserted", count)

    # Save to S3
    s3_path = save_to_s3(listings)
    if s3_path:
        logger.info("S3: %s", s3_path)

    # Summary
    logger.info("=" * 60)
    logger.info("Historical data generation complete!")
    logger.info("  Total rows: %d", count)
    logger.info("  Date range: %d days", days)
    logger.info("  Products: %d Best Buy + %d eBay variants",
                sum(len(v) for v in PRODUCT_CATALOG.values()),
                sum(len(v) for v in EBAY_VARIANTS.values()))
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
