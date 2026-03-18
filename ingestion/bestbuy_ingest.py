"""
PriceRadar - Synthetic Best Buy Data Generator

Generates realistic Best Buy-style product listings for electronics categories.
Used because Best Buy API requires corporate email access (blocks free/.edu emails).

The synthetic data matches the exact same schema as the raw_listings handoff contract,
so all downstream pipeline components (Spark, LLM matching, dbt, dashboards) work
identically to how they would with real Best Buy API data.

Owner: Chris (Track A)
"""

import json
import logging
import os
import random
import uuid
from datetime import datetime, timezone
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
logger = logging.getLogger("bestbuy_ingest")

# ---------------------------------------------------------------------------
# Realistic product catalog
# Each category contains real product names, brands, model numbers, and
# realistic price ranges that mirror actual Best Buy inventory.
# ---------------------------------------------------------------------------
PRODUCT_CATALOG: dict[str, list[dict[str, Any]]] = {
    "TVs": [
        {"name": "Samsung - 65\" Class QN85D Neo QLED 4K Smart TV", "brand": "Samsung", "model": "QN65QN85D", "min_price": 1099.99, "max_price": 1299.99},
        {"name": "Samsung - 55\" Class QN85D Neo QLED 4K Smart TV", "brand": "Samsung", "model": "QN55QN85D", "min_price": 899.99, "max_price": 1099.99},
        {"name": "Samsung - 75\" Class Q80D QLED 4K Smart TV", "brand": "Samsung", "model": "QN75Q80D", "min_price": 1299.99, "max_price": 1499.99},
        {"name": "LG - 65\" Class C4 Series OLED 4K UHD Smart TV", "brand": "LG", "model": "OLED65C4PUA", "min_price": 1499.99, "max_price": 1799.99},
        {"name": "LG - 55\" Class C4 Series OLED 4K UHD Smart TV", "brand": "LG", "model": "OLED55C4PUA", "min_price": 1099.99, "max_price": 1399.99},
        {"name": "LG - 77\" Class G4 Series OLED 4K UHD Smart TV", "brand": "LG", "model": "OLED77G4PUA", "min_price": 2999.99, "max_price": 3299.99},
        {"name": "Sony - 65\" Class BRAVIA XR A95L QD-OLED 4K Smart TV", "brand": "Sony", "model": "XR65A95L", "min_price": 2499.99, "max_price": 2799.99},
        {"name": "Sony - 55\" Class BRAVIA 7 LED 4K Smart TV", "brand": "Sony", "model": "K55XR70", "min_price": 999.99, "max_price": 1199.99},
        {"name": "TCL - 65\" Class Q7 Q-Series QLED 4K Smart TV", "brand": "TCL", "model": "65Q750G", "min_price": 549.99, "max_price": 699.99},
        {"name": "TCL - 55\" Class S4 S-Series LED 4K Smart TV", "brand": "TCL", "model": "55S450G", "min_price": 249.99, "max_price": 329.99},
        {"name": "Hisense - 65\" Class U8N Mini-LED 4K Smart TV", "brand": "Hisense", "model": "65U8N", "min_price": 899.99, "max_price": 1099.99},
        {"name": "Hisense - 75\" Class U7N Mini-LED 4K Smart TV", "brand": "Hisense", "model": "75U7N", "min_price": 999.99, "max_price": 1199.99},
        {"name": "Vizio - 65\" Class M-Series Quantum 4K Smart TV", "brand": "Vizio", "model": "M65Q6-L4", "min_price": 449.99, "max_price": 549.99},
        {"name": "Samsung - 85\" Class QN90D Neo QLED 4K Smart TV", "brand": "Samsung", "model": "QN85QN90D", "min_price": 2799.99, "max_price": 3299.99},
        {"name": "LG - 65\" Class B4 Series OLED 4K UHD Smart TV", "brand": "LG", "model": "OLED65B4PUA", "min_price": 999.99, "max_price": 1299.99},
    ],
    "Laptops": [
        {"name": "Apple MacBook Air 15\" Laptop - M3 chip - 16GB Memory - 256GB SSD", "brand": "Apple", "model": "MRYQ3LL/A", "min_price": 1099.99, "max_price": 1299.99},
        {"name": "Apple MacBook Pro 14\" Laptop - M4 Pro chip - 24GB Memory - 512GB SSD", "brand": "Apple", "model": "MX4G3LL/A", "min_price": 1799.99, "max_price": 1999.99},
        {"name": "Dell - XPS 14 14\" OLED Touch Laptop - Intel Core Ultra 7 - 16GB Memory - 512GB SSD", "brand": "Dell", "model": "XPS9440", "min_price": 1399.99, "max_price": 1599.99},
        {"name": "Dell - Inspiron 15 15.6\" FHD Touch Laptop - Intel Core i7 - 16GB Memory - 512GB SSD", "brand": "Dell", "model": "I5530-7845SLV", "min_price": 699.99, "max_price": 849.99},
        {"name": "HP - OMEN 16.1\" Gaming Laptop - Intel Core i9 - 32GB Memory - NVIDIA RTX 4070 - 1TB SSD", "brand": "HP", "model": "16-wf1075cl", "min_price": 1499.99, "max_price": 1699.99},
        {"name": "HP - Spectre x360 14\" 2-in-1 OLED Touch Laptop - Intel Core Ultra 7 - 16GB Memory - 1TB SSD", "brand": "HP", "model": "14-eu0023dx", "min_price": 1299.99, "max_price": 1499.99},
        {"name": "Lenovo - ThinkPad X1 Carbon Gen 12 14\" Touch Laptop - Intel Core Ultra 7 - 16GB Memory - 512GB SSD", "brand": "Lenovo", "model": "21KC005YUS", "min_price": 1399.99, "max_price": 1649.99},
        {"name": "Lenovo - IdeaPad Slim 5 16\" Laptop - AMD Ryzen 7 - 16GB Memory - 512GB SSD", "brand": "Lenovo", "model": "82XF002AUS", "min_price": 599.99, "max_price": 749.99},
        {"name": "ASUS - ROG Strix G16 16\" Gaming Laptop - Intel Core i9 - 16GB Memory - NVIDIA RTX 4060 - 1TB SSD", "brand": "ASUS", "model": "G614JU-ES94", "min_price": 1199.99, "max_price": 1399.99},
        {"name": "ASUS - Zenbook 14 OLED 14\" Laptop - Intel Core Ultra 7 - 16GB Memory - 512GB SSD", "brand": "ASUS", "model": "UX3405MA-DS76", "min_price": 999.99, "max_price": 1149.99},
        {"name": "Acer - Nitro V 15.6\" Gaming Laptop - AMD Ryzen 5 - 8GB Memory - NVIDIA RTX 4050 - 512GB SSD", "brand": "Acer", "model": "ANV15-41-R5FN", "min_price": 699.99, "max_price": 849.99},
        {"name": "Microsoft - Surface Laptop 7 13.8\" Touch Laptop - Snapdragon X Plus - 16GB Memory - 256GB SSD", "brand": "Microsoft", "model": "ZHI-00001", "min_price": 999.99, "max_price": 1099.99},
    ],
    "Headphones": [
        {"name": "Sony - WH-1000XM5 Wireless Noise Cancelling Over-Ear Headphones", "brand": "Sony", "model": "WH1000XM5/B", "min_price": 299.99, "max_price": 399.99},
        {"name": "Sony - WF-1000XM5 True Wireless Noise Cancelling Earbuds", "brand": "Sony", "model": "WF1000XM5/B", "min_price": 249.99, "max_price": 299.99},
        {"name": "Apple - AirPods Pro 2nd Generation with USB-C", "brand": "Apple", "model": "MTJV3AM/A", "min_price": 189.99, "max_price": 249.99},
        {"name": "Apple - AirPods Max - USB-C", "brand": "Apple", "model": "MUW63AM/A", "min_price": 449.99, "max_price": 549.99},
        {"name": "Bose - QuietComfort Ultra Wireless Noise Cancelling Over-Ear Headphones", "brand": "Bose", "model": "880066-0100", "min_price": 349.99, "max_price": 429.99},
        {"name": "Bose - QuietComfort Ultra Earbuds True Wireless Noise Cancelling", "brand": "Bose", "model": "882826-0010", "min_price": 249.99, "max_price": 299.99},
        {"name": "Samsung - Galaxy Buds3 Pro True Wireless Noise Cancelling Earbuds", "brand": "Samsung", "model": "SM-R630NZAAXAR", "min_price": 199.99, "max_price": 249.99},
        {"name": "Beats - Studio Pro Wireless Noise Cancelling Over-Ear Headphones", "brand": "Beats", "model": "MQTP3LL/A", "min_price": 249.99, "max_price": 349.99},
        {"name": "JBL - Tour One M2 Wireless Noise Cancelling Over-Ear Headphones", "brand": "JBL", "model": "JBLTOURONEM2BLK", "min_price": 249.99, "max_price": 299.99},
        {"name": "Sennheiser - Momentum 4 Wireless Noise Cancelling Over-Ear Headphones", "brand": "Sennheiser", "model": "509267", "min_price": 299.99, "max_price": 379.99},
    ],
    "Smartwatches": [
        {"name": "Apple Watch Series 10 GPS 46mm Aluminum Case with Sport Band", "brand": "Apple", "model": "MXM23LL/A", "min_price": 399.99, "max_price": 429.99},
        {"name": "Apple Watch Series 10 GPS 42mm Aluminum Case with Sport Band", "brand": "Apple", "model": "MXM03LL/A", "min_price": 349.99, "max_price": 399.99},
        {"name": "Apple Watch Ultra 2 GPS + Cellular 49mm Titanium Case", "brand": "Apple", "model": "MQDY3LL/A", "min_price": 749.99, "max_price": 799.99},
        {"name": "Samsung - Galaxy Watch7 Smartwatch 44mm BT Aluminum Case", "brand": "Samsung", "model": "SM-L505DZGAXAA", "min_price": 279.99, "max_price": 329.99},
        {"name": "Samsung - Galaxy Watch Ultra Smartwatch 47mm Titanium Case", "brand": "Samsung", "model": "SM-L705DZTAXAA", "min_price": 599.99, "max_price": 649.99},
        {"name": "Google - Pixel Watch 3 45mm Smartwatch with Obsidian Active Band", "brand": "Google", "model": "GA05764-US", "min_price": 349.99, "max_price": 399.99},
        {"name": "Garmin - Venu 3 GPS Smartwatch 45mm AMOLED", "brand": "Garmin", "model": "010-02784-01", "min_price": 399.99, "max_price": 449.99},
        {"name": "Fitbit - Sense 2 Health and Fitness Smartwatch", "brand": "Fitbit", "model": "FB521BKGB", "min_price": 199.99, "max_price": 249.99},
    ],
    "Tablets": [
        {"name": "Apple - iPad Pro 11\" M4 chip Wi-Fi 256GB", "brand": "Apple", "model": "MW5H3LL/A", "min_price": 999.99, "max_price": 1099.99},
        {"name": "Apple - iPad Air 13\" M2 chip Wi-Fi 128GB", "brand": "Apple", "model": "MV273LL/A", "min_price": 749.99, "max_price": 799.99},
        {"name": "Apple - iPad 10.9\" 10th Gen Wi-Fi 64GB", "brand": "Apple", "model": "MPQ03LL/A", "min_price": 329.99, "max_price": 349.99},
        {"name": "Samsung - Galaxy Tab S10 Ultra 14.6\" 256GB Wi-Fi", "brand": "Samsung", "model": "SM-X820NZAAXAR", "min_price": 1099.99, "max_price": 1199.99},
        {"name": "Samsung - Galaxy Tab S10+ 12.4\" 256GB Wi-Fi", "brand": "Samsung", "model": "SM-X820NZAAXAR", "min_price": 899.99, "max_price": 999.99},
        {"name": "Samsung - Galaxy Tab A9+ 11\" 64GB Wi-Fi", "brand": "Samsung", "model": "SM-X210NZAAXAR", "min_price": 199.99, "max_price": 269.99},
        {"name": "Microsoft - Surface Pro 11th Edition 13\" Snapdragon X Plus - 16GB Memory - 256GB SSD", "brand": "Microsoft", "model": "ZHY-00001", "min_price": 999.99, "max_price": 1099.99},
        {"name": "Lenovo - Tab P12 12.7\" 128GB Wi-Fi", "brand": "Lenovo", "model": "ZACH0120US", "min_price": 249.99, "max_price": 299.99},
        {"name": "Amazon - Fire Max 11 11\" Tablet 64GB", "brand": "Amazon", "model": "T8S26B", "min_price": 199.99, "max_price": 229.99},
    ],
}


def _generate_sku() -> str:
    """Generate a realistic Best Buy-style SKU (7-digit number)."""
    return str(random.randint(6000000, 6999999))


def _generate_price(product: dict[str, Any]) -> tuple[Decimal, Decimal | None, bool]:
    """
    Generate a realistic price with occasional sales.
    Returns (regular_price, sale_price_or_none, is_on_sale).
    """
    regular_price = Decimal(str(round(
        random.uniform(product["min_price"], product["max_price"]), 2
    )))

    # ~30% chance of being on sale
    on_sale = random.random() < 0.30
    if on_sale:
        discount_pct = random.uniform(0.05, 0.25)  # 5-25% off
        sale_price = Decimal(str(round(float(regular_price) * (1 - discount_pct), 2)))
    else:
        sale_price = None

    return regular_price, sale_price, on_sale


def _generate_url(sku: str, product_name: str) -> str:
    """Generate a realistic Best Buy product URL."""
    slug = product_name.lower()
    for ch in ['"', "'", "(", ")", ",", ".", "/", "&", "-", "  "]:
        slug = slug.replace(ch, " ")
    slug = "-".join(slug.split())[:80]
    return f"https://www.bestbuy.com/site/{slug}/{sku}.p?skuId={sku}"


def generate_listings(products_per_category: int = 10) -> list[dict[str, Any]]:
    """
    Generate synthetic Best Buy product listings.

    Args:
        products_per_category: Number of products to generate per category.
                               If > catalog size, products are repeated with price variation.

    Returns:
        List of product dicts matching the raw_listings schema.
    """
    now = datetime.now(timezone.utc)
    listings: list[dict[str, Any]] = []

    for category, products in PRODUCT_CATALOG.items():
        selected = random.choices(products, k=min(products_per_category, len(products)))

        for product in selected:
            sku = _generate_sku()
            regular_price, sale_price, on_sale = _generate_price(product)

            listing = {
                "product_name": product["name"],
                "price": float(regular_price),
                "sale_price": float(sale_price) if sale_price else None,
                "on_sale": on_sale,
                "source": "bestbuy",
                "url": _generate_url(sku, product["name"]),
                "category": category,
                "brand": product["brand"],
                "sku": sku,
                "scraped_at": now.isoformat(),
                "raw_payload": {
                    "sku": sku,
                    "name": product["name"],
                    "regularPrice": float(regular_price),
                    "salePrice": float(sale_price) if sale_price else float(regular_price),
                    "onSale": on_sale,
                    "manufacturer": product["brand"],
                    "modelNumber": product["model"],
                    "categoryPath": [{"id": category, "name": category}],
                    "url": _generate_url(sku, product["name"]),
                    "inStoreAvailability": random.choice([True, True, True, False]),
                    "onlineAvailability": True,
                    "customerReviewAverage": round(random.uniform(3.5, 5.0), 1),
                    "customerReviewCount": random.randint(10, 5000),
                    "_synthetic": True,
                    "_generated_at": now.isoformat(),
                },
            }
            listings.append(listing)

    logger.info("Generated %d synthetic Best Buy listings across %d categories",
                len(listings), len(PRODUCT_CATALOG))
    return listings


def save_to_s3(listings: list[dict[str, Any]]) -> str | None:
    """Save raw JSON to S3 at raw/bestbuy/{iso_timestamp}.json."""
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        logger.warning("S3_BUCKET_NAME not set — skipping S3 upload")
        return None

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    key = f"raw/bestbuy/{timestamp}.json"

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
        logger.info("Uploaded %d listings to s3://%s/%s", len(listings), bucket, key)
        return f"s3://{bucket}/{key}"
    except Exception:
        logger.exception("Failed to upload to S3")
        return None


def save_to_postgres(listings: list[dict[str, Any]]) -> int:
    """Insert listings into PostgreSQL raw_listings table."""
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "priceradar")
    pg_user = os.getenv("POSTGRES_USER", "priceradar")
    pg_pass = os.getenv("POSTGRES_PASSWORD", "priceradar_dev")

    connection_string = f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

    try:
        engine = create_engine(connection_string)
        inserted = 0

        with engine.begin() as conn:
            for listing in listings:
                conn.execute(
                    text("""
                        INSERT INTO raw_listings
                            (product_name, price, sale_price, on_sale, source,
                             url, category, brand, sku, scraped_at, raw_payload)
                        VALUES
                            (:product_name, :price, :sale_price, :on_sale, :source,
                             :url, :category, :brand, :sku, NOW(),
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
                        "raw_payload": json.dumps(listing["raw_payload"]),
                    },
                )
                inserted += 1

        logger.info("Inserted %d listings into PostgreSQL raw_listings", inserted)
        return inserted

    except Exception:
        logger.exception("Failed to insert into PostgreSQL")
        return 0


def main(products_per_category: int = 10) -> None:
    """
    Main entry point: generate synthetic Best Buy data, save to S3 + Postgres.

    Args:
        products_per_category: Number of products per category to generate.
    """
    logger.info("Starting synthetic Best Buy data generation")
    listings = generate_listings(products_per_category=products_per_category)

    # Save to S3
    s3_path = save_to_s3(listings)
    if s3_path:
        logger.info("S3 upload complete: %s", s3_path)

    # Save to PostgreSQL
    count = save_to_postgres(listings)
    logger.info("PostgreSQL insert complete: %d rows", count)

    # Also save a local copy for Punith's test fixtures
    local_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "tests", "fixtures", "sample_bestbuy.json",
    )
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w") as f:
        json.dump(listings[:5], f, indent=2, default=str)
    logger.info("Saved sample fixtures to %s", local_path)

    logger.info("Best Buy synthetic generation complete — %d total listings", len(listings))


if __name__ == "__main__":
    main()