"""
PriceRadar — Load sample fixtures + generate synthetic data into local PostgreSQL.
Run this once to populate your local Postgres with enough data for dbt, dashboards, and ML.

Usage:
    python scripts/load_fixtures.py

Requires:
    - PostgreSQL running via docker compose (docker compose up -d postgres)
    - Environment: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
      (defaults match docker-compose.yml)
"""

import json
import random
import os
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import sqlalchemy as sa
from sqlalchemy import text

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- Config ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "priceradar")
POSTGRES_USER = os.getenv("POSTGRES_USER", "priceradar")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "priceradar_dev")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# --- Synthetic product catalog ---
PRODUCTS = {
    "TVs": [
        {"name": "Samsung - 65\" Class QN85D Neo QLED 4K Smart TV", "brand": "Samsung", "base_price": 1299.99},
        {"name": "LG - 55\" Class C4 Series OLED 4K UHD Smart TV", "brand": "LG", "base_price": 1499.99},
        {"name": "Sony - 75\" Class BRAVIA XR A95L OLED 4K Smart TV", "brand": "Sony", "base_price": 2799.99},
        {"name": "TCL - 65\" Class S4 S-Class 4K UHD HDR LED Smart TV", "brand": "TCL", "base_price": 399.99},
        {"name": "Hisense - 55\" Class U8K Mini-LED ULED 4K Smart TV", "brand": "Hisense", "base_price": 749.99},
        {"name": "Samsung - 55\" Class S90D OLED 4K Smart TV", "brand": "Samsung", "base_price": 1599.99},
        {"name": "LG - 65\" Class B4 Series OLED 4K UHD Smart TV", "brand": "LG", "base_price": 1299.99},
        {"name": "Sony - 65\" Class BRAVIA 7 LED 4K Smart TV", "brand": "Sony", "base_price": 1499.99},
        {"name": "Vizio - 75\" Class M-Series Quantum 4K HDR Smart TV", "brand": "Vizio", "base_price": 699.99},
        {"name": "TCL - 75\" Class QM8 QLED 4K Mini-LED Smart TV", "brand": "TCL", "base_price": 1199.99},
    ],
    "Laptops": [
        {"name": "Apple MacBook Air 15\" M3 Chip 16GB 512GB", "brand": "Apple", "base_price": 1299.99},
        {"name": "Dell XPS 15 Intel Core Ultra 7 16GB 512GB", "brand": "Dell", "base_price": 1399.99},
        {"name": "HP Spectre x360 16\" Intel Core Ultra 7 16GB 1TB", "brand": "HP", "base_price": 1599.99},
        {"name": "Lenovo ThinkPad X1 Carbon Gen 12 Intel Core Ultra 7", "brand": "Lenovo", "base_price": 1549.99},
        {"name": "ASUS ROG Zephyrus G16 RTX 4070 16GB 1TB", "brand": "ASUS", "base_price": 1799.99},
        {"name": "Acer Swift Go 14 Intel Core Ultra 5 16GB 512GB", "brand": "Acer", "base_price": 799.99},
        {"name": "Microsoft Surface Laptop 6 Intel Core Ultra 7 16GB", "brand": "Microsoft", "base_price": 1499.99},
        {"name": "Samsung Galaxy Book4 Pro 16\" Intel Core Ultra 7", "brand": "Samsung", "base_price": 1449.99},
        {"name": "HP Pavilion 15 AMD Ryzen 7 16GB 512GB", "brand": "HP", "base_price": 699.99},
        {"name": "Lenovo IdeaPad 5 15\" AMD Ryzen 5 8GB 256GB", "brand": "Lenovo", "base_price": 549.99},
    ],
    "Headphones": [
        {"name": "Sony WH-1000XM5 Wireless Noise Canceling Headphones", "brand": "Sony", "base_price": 349.99},
        {"name": "Apple AirPods Max (USB-C)", "brand": "Apple", "base_price": 549.99},
        {"name": "Bose QuietComfort Ultra Headphones", "brand": "Bose", "base_price": 429.99},
        {"name": "Samsung Galaxy Buds3 Pro True Wireless", "brand": "Samsung", "base_price": 249.99},
        {"name": "Apple AirPods Pro 2 (USB-C) with MagSafe", "brand": "Apple", "base_price": 249.99},
        {"name": "Sony WF-1000XM5 True Wireless Earbuds", "brand": "Sony", "base_price": 299.99},
        {"name": "Bose QuietComfort Earbuds II", "brand": "Bose", "base_price": 279.99},
        {"name": "JBL Tour One M2 Wireless Noise Canceling", "brand": "JBL", "base_price": 299.99},
        {"name": "Sennheiser Momentum 4 Wireless Headphones", "brand": "Sennheiser", "base_price": 349.99},
        {"name": "Beats Studio Pro Wireless Noise Canceling", "brand": "Beats", "base_price": 349.99},
    ],
    "Smartwatches": [
        {"name": "Apple Watch Series 10 GPS 46mm", "brand": "Apple", "base_price": 429.99},
        {"name": "Samsung Galaxy Watch7 44mm GPS", "brand": "Samsung", "base_price": 329.99},
        {"name": "Apple Watch Ultra 2 GPS + Cellular 49mm", "brand": "Apple", "base_price": 799.99},
        {"name": "Google Pixel Watch 3 41mm WiFi", "brand": "Google", "base_price": 349.99},
        {"name": "Garmin Venu 3 GPS Smartwatch", "brand": "Garmin", "base_price": 449.99},
        {"name": "Samsung Galaxy Watch Ultra 47mm GPS", "brand": "Samsung", "base_price": 649.99},
        {"name": "Fitbit Sense 2 Health Smartwatch", "brand": "Fitbit", "base_price": 249.99},
        {"name": "Garmin Fenix 8 Solar 51mm", "brand": "Garmin", "base_price": 999.99},
    ],
    "Tablets": [
        {"name": "Apple iPad Pro 13\" M4 Chip 256GB WiFi", "brand": "Apple", "base_price": 1299.99},
        {"name": "Samsung Galaxy Tab S10 Ultra 14.6\" 256GB", "brand": "Samsung", "base_price": 1199.99},
        {"name": "Apple iPad Air 13\" M2 Chip 128GB WiFi", "brand": "Apple", "base_price": 799.99},
        {"name": "Microsoft Surface Pro 11 Intel Core Ultra 7", "brand": "Microsoft", "base_price": 1499.99},
        {"name": "Samsung Galaxy Tab S10+ 12.4\" 256GB", "brand": "Samsung", "base_price": 999.99},
        {"name": "Apple iPad 10th Gen 10.9\" 64GB WiFi", "brand": "Apple", "base_price": 349.99},
        {"name": "Lenovo Tab P12 Pro 12.6\" 256GB", "brand": "Lenovo", "base_price": 499.99},
        {"name": "Amazon Fire Max 11 64GB", "brand": "Amazon", "base_price": 229.99},
    ],
}


def generate_sku(source: str, index: int) -> str:
    """Generate a realistic SKU based on source."""
    if source == "bestbuy":
        return str(6500000 + index)
    else:
        return f"v1|{300000000000 + index}|0"


def generate_url(source: str, product_name: str, sku: str) -> str:
    """Generate a realistic product URL."""
    slug = product_name.lower().replace(" ", "-").replace('"', "")[:60]
    if source == "bestbuy":
        return f"https://www.bestbuy.com/site/{slug}/{sku}.p?skuId={sku}"
    else:
        item_id = sku.split("|")[1] if "|" in sku else sku
        return f"https://www.ebay.com/itm/{item_id}"


def generate_synthetic_rows(num_days: int = 30) -> list[dict]:
    """Generate synthetic price snapshot data across multiple days."""
    rows = []
    sources = ["ebay", "bestbuy"]
    now = datetime.now(timezone.utc)
    idx = 0

    for day_offset in range(num_days, 0, -1):
        snapshot_time = now - timedelta(days=day_offset)
        # Add some hour variation (6am, 12pm, 6pm, midnight runs)
        for hour_offset in [0, 6, 12, 18]:
            scraped_at = snapshot_time.replace(hour=hour_offset, minute=0, second=0)

            for category, products in PRODUCTS.items():
                for product in products:
                    for source in sources:
                        idx += 1
                        base = product["base_price"]

                        # Add price variation: +/- 15% random noise over time
                        day_factor = 1.0 + 0.05 * random.uniform(-1, 1)
                        source_factor = 0.95 if source == "ebay" else 1.0  # eBay slightly cheaper

                        price = round(base * day_factor * source_factor, 2)

                        # 30% chance of being on sale
                        on_sale = random.random() < 0.30
                        if on_sale:
                            discount = random.uniform(0.05, 0.25)
                            sale_price = round(price * (1 - discount), 2)
                        else:
                            sale_price = None

                        # Inject anomalies: 2% chance of a big price drop
                        if random.random() < 0.02:
                            price = round(base * random.uniform(0.5, 0.7), 2)
                            on_sale = True
                            sale_price = price

                        sku = generate_sku(source, idx)
                        url = generate_url(source, product["name"], sku)

                        raw_payload = {
                            "original_name": product["name"],
                            "base_price": base,
                            "source": source,
                            "_synthetic": True,
                            "_generated_at": scraped_at.isoformat(),
                        }

                        rows.append({
                            "product_name": product["name"],
                            "price": price,
                            "sale_price": sale_price,
                            "on_sale": on_sale,
                            "source": source,
                            "url": url,
                            "category": category,
                            "brand": product["brand"],
                            "sku": sku,
                            "scraped_at": scraped_at,
                            "raw_payload": json.dumps(raw_payload),
                        })

    # Only keep a random subset to avoid massive data — aim for ~3000-4000 rows
    # Sample 1 out of every 3 snapshots
    random.shuffle(rows)
    rows = rows[:4000]
    logger.info(f"Generated {len(rows)} synthetic rows across {num_days} days")
    return rows


def load_fixtures(engine: sa.Engine) -> int:
    """Load the sample JSON fixtures from tests/fixtures/."""
    fixtures_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tests", "fixtures")
    total = 0

    for filename in ["sample_bestbuy.json", "sample_ebay.json"]:
        filepath = os.path.join(fixtures_dir, filename)
        if not os.path.exists(filepath):
            logger.warning(f"Fixture not found: {filepath}")
            continue

        with open(filepath, "r") as f:
            records = json.load(f)

        with engine.begin() as conn:
            for record in records:
                conn.execute(
                    text("""
                        INSERT INTO raw_listings 
                            (product_name, price, sale_price, on_sale, source, url, 
                             category, brand, sku, scraped_at, raw_payload)
                        VALUES 
                            (:product_name, :price, :sale_price, :on_sale, :source, :url,
                             :category, :brand, :sku, :scraped_at, :raw_payload)
                    """),
                    {
                        "product_name": record["product_name"],
                        "price": record["price"],
                        "sale_price": record.get("sale_price"),
                        "on_sale": record.get("on_sale", False),
                        "source": record["source"],
                        "url": record.get("url"),
                        "category": record.get("category"),
                        "brand": record.get("brand"),
                        "sku": record.get("sku"),
                        "scraped_at": record.get("scraped_at"),
                        "raw_payload": json.dumps(record.get("raw_payload", {})),
                    },
                )
                total += 1

        logger.info(f"Loaded {len(records)} rows from {filename}")

    return total


def load_synthetic(engine: sa.Engine) -> int:
    """Generate and load synthetic data."""
    rows = generate_synthetic_rows(num_days=30)

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text("""
                    INSERT INTO raw_listings 
                        (product_name, price, sale_price, on_sale, source, url, 
                         category, brand, sku, scraped_at, raw_payload)
                    VALUES 
                        (:product_name, :price, :sale_price, :on_sale, :source, :url,
                         :category, :brand, :sku, :scraped_at, :raw_payload)
                """),
                row,
            )

    logger.info(f"Loaded {len(rows)} synthetic rows")
    return len(rows)


def main():
    logger.info(f"Connecting to PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    engine = sa.create_engine(DATABASE_URL)

    # Test connection
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM raw_listings"))
        existing = result.scalar()
        logger.info(f"Existing rows in raw_listings: {existing}")

    if existing > 0:
        logger.info("Data already exists. Clearing table for fresh load...")
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM matched_products"))
            conn.execute(text("DELETE FROM raw_listings"))

    # Load fixtures first
    fixture_count = load_fixtures(engine)
    logger.info(f"Loaded {fixture_count} fixture rows")

    # Generate and load synthetic data
    synthetic_count = load_synthetic(engine)
    logger.info(f"Loaded {synthetic_count} synthetic rows")

    # Summary
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT source, category, COUNT(*) as cnt 
            FROM raw_listings 
            GROUP BY source, category 
            ORDER BY source, category
        """))
        logger.info("=== Final data summary ===")
        for row in result:
            logger.info(f"  {row[0]:10s} | {row[1]:15s} | {row[2]:5d} rows")

        total = conn.execute(text("SELECT COUNT(*) FROM raw_listings")).scalar()
        logger.info(f"  TOTAL: {total} rows")


if __name__ == "__main__":
    main()
