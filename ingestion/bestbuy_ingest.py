"""
PriceRadar - Best Buy listing generator.

Best Buy's developer API requires a corporate email to register, so this module
produces Best Buy-style listings instead: a catalog of 54 real products with realistic
price ranges, random regular prices and occasional sales. The output has the same shape
as the real API and the same raw_listings schema as eBay, so everything downstream
treats both sources the same way. Every generated row is flagged `_synthetic: true` in
`raw_payload`.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from common.db import insert_listings
from common.log import get_logger
from common.storage import upload_json

load_dotenv()
logger = get_logger("bestbuy_ingest")

# Real Best Buy products (names, brands, model numbers) with realistic price ranges.
PRODUCT_CATALOG: dict[str, list[dict[str, Any]]] = {
    "TVs": [
        {
            "name": 'Samsung - 65" Class QN85D Neo QLED 4K Smart TV',
            "brand": "Samsung",
            "model": "QN65QN85D",
            "min_price": 1099.99,
            "max_price": 1299.99,
        },
        {
            "name": 'Samsung - 55" Class QN85D Neo QLED 4K Smart TV',
            "brand": "Samsung",
            "model": "QN55QN85D",
            "min_price": 899.99,
            "max_price": 1099.99,
        },
        {
            "name": 'Samsung - 75" Class Q80D QLED 4K Smart TV',
            "brand": "Samsung",
            "model": "QN75Q80D",
            "min_price": 1299.99,
            "max_price": 1499.99,
        },
        {
            "name": 'LG - 65" Class C4 Series OLED 4K UHD Smart TV',
            "brand": "LG",
            "model": "OLED65C4PUA",
            "min_price": 1499.99,
            "max_price": 1799.99,
        },
        {
            "name": 'LG - 55" Class C4 Series OLED 4K UHD Smart TV',
            "brand": "LG",
            "model": "OLED55C4PUA",
            "min_price": 1099.99,
            "max_price": 1399.99,
        },
        {
            "name": 'LG - 77" Class G4 Series OLED 4K UHD Smart TV',
            "brand": "LG",
            "model": "OLED77G4PUA",
            "min_price": 2999.99,
            "max_price": 3299.99,
        },
        {
            "name": 'Sony - 65" Class BRAVIA XR A95L QD-OLED 4K Smart TV',
            "brand": "Sony",
            "model": "XR65A95L",
            "min_price": 2499.99,
            "max_price": 2799.99,
        },
        {
            "name": 'Sony - 55" Class BRAVIA 7 LED 4K Smart TV',
            "brand": "Sony",
            "model": "K55XR70",
            "min_price": 999.99,
            "max_price": 1199.99,
        },
        {
            "name": 'TCL - 65" Class Q7 Q-Series QLED 4K Smart TV',
            "brand": "TCL",
            "model": "65Q750G",
            "min_price": 549.99,
            "max_price": 699.99,
        },
        {
            "name": 'TCL - 55" Class S4 S-Series LED 4K Smart TV',
            "brand": "TCL",
            "model": "55S450G",
            "min_price": 249.99,
            "max_price": 329.99,
        },
        {
            "name": 'Hisense - 65" Class U8N Mini-LED 4K Smart TV',
            "brand": "Hisense",
            "model": "65U8N",
            "min_price": 899.99,
            "max_price": 1099.99,
        },
        {
            "name": 'Hisense - 75" Class U7N Mini-LED 4K Smart TV',
            "brand": "Hisense",
            "model": "75U7N",
            "min_price": 999.99,
            "max_price": 1199.99,
        },
        {
            "name": 'Vizio - 65" Class M-Series Quantum 4K Smart TV',
            "brand": "Vizio",
            "model": "M65Q6-L4",
            "min_price": 449.99,
            "max_price": 549.99,
        },
        {
            "name": 'Samsung - 85" Class QN90D Neo QLED 4K Smart TV',
            "brand": "Samsung",
            "model": "QN85QN90D",
            "min_price": 2799.99,
            "max_price": 3299.99,
        },
        {
            "name": 'LG - 65" Class B4 Series OLED 4K UHD Smart TV',
            "brand": "LG",
            "model": "OLED65B4PUA",
            "min_price": 999.99,
            "max_price": 1299.99,
        },
    ],
    "Laptops": [
        {
            "name": 'Apple MacBook Air 15" Laptop - M3 chip - 16GB Memory - 256GB SSD',
            "brand": "Apple",
            "model": "MRYQ3LL/A",
            "min_price": 1099.99,
            "max_price": 1299.99,
        },
        {
            "name": 'Apple MacBook Pro 14" Laptop - M4 Pro chip - 24GB Memory - 512GB SSD',
            "brand": "Apple",
            "model": "MX4G3LL/A",
            "min_price": 1799.99,
            "max_price": 1999.99,
        },
        {
            "name": 'Dell - XPS 14 14" OLED Touch Laptop - Intel Core Ultra 7 - 16GB Memory - 512GB SSD',
            "brand": "Dell",
            "model": "XPS9440",
            "min_price": 1399.99,
            "max_price": 1599.99,
        },
        {
            "name": 'Dell - Inspiron 15 15.6" FHD Touch Laptop - Intel Core i7 - 16GB Memory - 512GB SSD',
            "brand": "Dell",
            "model": "I5530-7845SLV",
            "min_price": 699.99,
            "max_price": 849.99,
        },
        {
            "name": 'HP - OMEN 16.1" Gaming Laptop - Intel Core i9 - 32GB Memory - NVIDIA RTX 4070 - 1TB SSD',
            "brand": "HP",
            "model": "16-wf1075cl",
            "min_price": 1499.99,
            "max_price": 1699.99,
        },
        {
            "name": 'HP - Spectre x360 14" 2-in-1 OLED Touch Laptop - Intel Core Ultra 7 - 16GB Memory - 1TB SSD',
            "brand": "HP",
            "model": "14-eu0023dx",
            "min_price": 1299.99,
            "max_price": 1499.99,
        },
        {
            "name": 'Lenovo - ThinkPad X1 Carbon Gen 12 14" Touch Laptop - Intel Core Ultra 7 - 16GB Memory - 512GB SSD',
            "brand": "Lenovo",
            "model": "21KC005YUS",
            "min_price": 1399.99,
            "max_price": 1649.99,
        },
        {
            "name": 'Lenovo - IdeaPad Slim 5 16" Laptop - AMD Ryzen 7 - 16GB Memory - 512GB SSD',
            "brand": "Lenovo",
            "model": "82XF002AUS",
            "min_price": 599.99,
            "max_price": 749.99,
        },
        {
            "name": 'ASUS - ROG Strix G16 16" Gaming Laptop - Intel Core i9 - 16GB Memory - NVIDIA RTX 4060 - 1TB SSD',
            "brand": "ASUS",
            "model": "G614JU-ES94",
            "min_price": 1199.99,
            "max_price": 1399.99,
        },
        {
            "name": 'ASUS - Zenbook 14 OLED 14" Laptop - Intel Core Ultra 7 - 16GB Memory - 512GB SSD',
            "brand": "ASUS",
            "model": "UX3405MA-DS76",
            "min_price": 999.99,
            "max_price": 1149.99,
        },
        {
            "name": 'Acer - Nitro V 15.6" Gaming Laptop - AMD Ryzen 5 - 8GB Memory - NVIDIA RTX 4050 - 512GB SSD',
            "brand": "Acer",
            "model": "ANV15-41-R5FN",
            "min_price": 699.99,
            "max_price": 849.99,
        },
        {
            "name": 'Microsoft - Surface Laptop 7 13.8" Touch Laptop - Snapdragon X Plus - 16GB Memory - 256GB SSD',
            "brand": "Microsoft",
            "model": "ZHI-00001",
            "min_price": 999.99,
            "max_price": 1099.99,
        },
    ],
    "Headphones": [
        {
            "name": "Sony - WH-1000XM5 Wireless Noise Cancelling Over-Ear Headphones",
            "brand": "Sony",
            "model": "WH1000XM5/B",
            "min_price": 299.99,
            "max_price": 399.99,
        },
        {
            "name": "Sony - WF-1000XM5 True Wireless Noise Cancelling Earbuds",
            "brand": "Sony",
            "model": "WF1000XM5/B",
            "min_price": 249.99,
            "max_price": 299.99,
        },
        {
            "name": "Apple - AirPods Pro 2nd Generation with USB-C",
            "brand": "Apple",
            "model": "MTJV3AM/A",
            "min_price": 189.99,
            "max_price": 249.99,
        },
        {
            "name": "Apple - AirPods Max - USB-C",
            "brand": "Apple",
            "model": "MUW63AM/A",
            "min_price": 449.99,
            "max_price": 549.99,
        },
        {
            "name": "Bose - QuietComfort Ultra Wireless Noise Cancelling Over-Ear Headphones",
            "brand": "Bose",
            "model": "880066-0100",
            "min_price": 349.99,
            "max_price": 429.99,
        },
        {
            "name": "Bose - QuietComfort Ultra Earbuds True Wireless Noise Cancelling",
            "brand": "Bose",
            "model": "882826-0010",
            "min_price": 249.99,
            "max_price": 299.99,
        },
        {
            "name": "Samsung - Galaxy Buds3 Pro True Wireless Noise Cancelling Earbuds",
            "brand": "Samsung",
            "model": "SM-R630NZAAXAR",
            "min_price": 199.99,
            "max_price": 249.99,
        },
        {
            "name": "Beats - Studio Pro Wireless Noise Cancelling Over-Ear Headphones",
            "brand": "Beats",
            "model": "MQTP3LL/A",
            "min_price": 249.99,
            "max_price": 349.99,
        },
        {
            "name": "JBL - Tour One M2 Wireless Noise Cancelling Over-Ear Headphones",
            "brand": "JBL",
            "model": "JBLTOURONEM2BLK",
            "min_price": 249.99,
            "max_price": 299.99,
        },
        {
            "name": "Sennheiser - Momentum 4 Wireless Noise Cancelling Over-Ear Headphones",
            "brand": "Sennheiser",
            "model": "509267",
            "min_price": 299.99,
            "max_price": 379.99,
        },
    ],
    "Smartwatches": [
        {
            "name": "Apple Watch Series 10 GPS 46mm Aluminum Case with Sport Band",
            "brand": "Apple",
            "model": "MXM23LL/A",
            "min_price": 399.99,
            "max_price": 429.99,
        },
        {
            "name": "Apple Watch Series 10 GPS 42mm Aluminum Case with Sport Band",
            "brand": "Apple",
            "model": "MXM03LL/A",
            "min_price": 349.99,
            "max_price": 399.99,
        },
        {
            "name": "Apple Watch Ultra 2 GPS + Cellular 49mm Titanium Case",
            "brand": "Apple",
            "model": "MQDY3LL/A",
            "min_price": 749.99,
            "max_price": 799.99,
        },
        {
            "name": "Samsung - Galaxy Watch7 Smartwatch 44mm BT Aluminum Case",
            "brand": "Samsung",
            "model": "SM-L505DZGAXAA",
            "min_price": 279.99,
            "max_price": 329.99,
        },
        {
            "name": "Samsung - Galaxy Watch Ultra Smartwatch 47mm Titanium Case",
            "brand": "Samsung",
            "model": "SM-L705DZTAXAA",
            "min_price": 599.99,
            "max_price": 649.99,
        },
        {
            "name": "Google - Pixel Watch 3 45mm Smartwatch with Obsidian Active Band",
            "brand": "Google",
            "model": "GA05764-US",
            "min_price": 349.99,
            "max_price": 399.99,
        },
        {
            "name": "Garmin - Venu 3 GPS Smartwatch 45mm AMOLED",
            "brand": "Garmin",
            "model": "010-02784-01",
            "min_price": 399.99,
            "max_price": 449.99,
        },
        {
            "name": "Fitbit - Sense 2 Health and Fitness Smartwatch",
            "brand": "Fitbit",
            "model": "FB521BKGB",
            "min_price": 199.99,
            "max_price": 249.99,
        },
    ],
    "Tablets": [
        {
            "name": 'Apple - iPad Pro 11" M4 chip Wi-Fi 256GB',
            "brand": "Apple",
            "model": "MW5H3LL/A",
            "min_price": 999.99,
            "max_price": 1099.99,
        },
        {
            "name": 'Apple - iPad Air 13" M2 chip Wi-Fi 128GB',
            "brand": "Apple",
            "model": "MV273LL/A",
            "min_price": 749.99,
            "max_price": 799.99,
        },
        {
            "name": 'Apple - iPad 10.9" 10th Gen Wi-Fi 64GB',
            "brand": "Apple",
            "model": "MPQ03LL/A",
            "min_price": 329.99,
            "max_price": 349.99,
        },
        {
            "name": 'Samsung - Galaxy Tab S10 Ultra 14.6" 256GB Wi-Fi',
            "brand": "Samsung",
            "model": "SM-X820NZAAXAR",
            "min_price": 1099.99,
            "max_price": 1199.99,
        },
        {
            "name": 'Samsung - Galaxy Tab S10+ 12.4" 256GB Wi-Fi',
            "brand": "Samsung",
            "model": "SM-X820NZAAXAR",
            "min_price": 899.99,
            "max_price": 999.99,
        },
        {
            "name": 'Samsung - Galaxy Tab A9+ 11" 64GB Wi-Fi',
            "brand": "Samsung",
            "model": "SM-X210NZAAXAR",
            "min_price": 199.99,
            "max_price": 269.99,
        },
        {
            "name": 'Microsoft - Surface Pro 11th Edition 13" Snapdragon X Plus - 16GB Memory - 256GB SSD',
            "brand": "Microsoft",
            "model": "ZHY-00001",
            "min_price": 999.99,
            "max_price": 1099.99,
        },
        {
            "name": 'Lenovo - Tab P12 12.7" 128GB Wi-Fi',
            "brand": "Lenovo",
            "model": "ZACH0120US",
            "min_price": 249.99,
            "max_price": 299.99,
        },
        {
            "name": 'Amazon - Fire Max 11 11" Tablet 64GB',
            "brand": "Amazon",
            "model": "T8S26B",
            "min_price": 199.99,
            "max_price": 229.99,
        },
    ],
}


SALE_PROBABILITY = 0.30
SALE_DISCOUNT_RANGE = (0.05, 0.25)


def _generate_sku(rng: random.Random) -> str:
    """Best Buy SKUs are 7-digit numbers."""
    return str(rng.randint(6000000, 6999999))


def _generate_price(
    product: dict[str, Any], rng: random.Random
) -> tuple[float, float | None, bool]:
    """Return (regular_price, sale_price or None, on_sale)."""
    regular = round(rng.uniform(product["min_price"], product["max_price"]), 2)
    if rng.random() < SALE_PROBABILITY:
        sale = round(regular * (1 - rng.uniform(*SALE_DISCOUNT_RANGE)), 2)
        return regular, sale, True
    return regular, None, False


def _product_url(sku: str, product_name: str) -> str:
    slug = product_name.lower()
    for ch in ['"', "'", "(", ")", ",", ".", "/", "&", "-"]:
        slug = slug.replace(ch, " ")
    slug = "-".join(slug.split())[:80]
    return f"https://www.bestbuy.com/site/{slug}/{sku}.p?skuId={sku}"


def generate_listings(
    products_per_category: int = 10, seed: int | None = None
) -> list[dict[str, Any]]:
    """Generate one listing per sampled catalog product.

    Products are sampled without replacement, so a run never lists the same product
    twice. `seed` makes the output reproducible (used by tests).
    """
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    listings: list[dict[str, Any]] = []

    for category, products in PRODUCT_CATALOG.items():
        for product in rng.sample(products, k=min(products_per_category, len(products))):
            sku = _generate_sku(rng)
            regular, sale, on_sale = _generate_price(product, rng)
            url = _product_url(sku, product["name"])
            listings.append(
                {
                    "product_name": product["name"],
                    "price": regular,
                    "sale_price": sale,
                    "on_sale": on_sale,
                    "source": "bestbuy",
                    "url": url,
                    "category": category,
                    "brand": product["brand"],
                    "sku": sku,
                    "scraped_at": now,
                    "raw_payload": {
                        "sku": sku,
                        "name": product["name"],
                        "regularPrice": regular,
                        "salePrice": sale if on_sale else regular,
                        "onSale": on_sale,
                        "manufacturer": product["brand"],
                        "modelNumber": product["model"],
                        "categoryPath": [{"id": category, "name": category}],
                        "url": url,
                        "inStoreAvailability": rng.random() < 0.75,
                        "onlineAvailability": True,
                        "customerReviewAverage": round(rng.uniform(3.5, 5.0), 1),
                        "customerReviewCount": rng.randint(10, 5000),
                        "_synthetic": True,
                    },
                }
            )

    logger.info(
        "Generated %d Best Buy listings across %d categories", len(listings), len(PRODUCT_CATALOG)
    )
    return listings


def main(products_per_category: int = 10, write_fixtures: bool = False) -> int:
    """Generate listings, archive them to S3 and insert into Postgres."""
    listings = generate_listings(products_per_category=products_per_category)
    upload_json(listings, "raw/bestbuy")
    inserted = insert_listings(listings)
    logger.info("Best Buy ingestion complete: %d listings inserted", inserted)

    if write_fixtures:
        path = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "sample_bestbuy.json"
        path.write_text(json.dumps(listings[:5], indent=2, default=str))
        logger.info("Wrote sample fixtures to %s", path)
    return inserted


if __name__ == "__main__":
    main(write_fixtures=True)
