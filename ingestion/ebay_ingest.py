"""
PriceRadar - eBay Browse API Ingestion

Fetches product listings from eBay Browse API across 5 electronics categories.
Uses OAuth2 client credentials flow for authentication.

IMPORTANT: This is inference/classification only. No eBay data is used to
fine-tune or train any AI model. The LLM SKU matcher performs real-time
classification at runtime. This complies with eBay's June 2025 API License
Agreement which prohibits using eBay Content to train AI systems.

Owner: Chris (Track A)
"""

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ebay_ingest")

# ---------------------------------------------------------------------------
# eBay API Configuration
# ---------------------------------------------------------------------------
EBAY_AUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_SCOPE = "https://api.ebay.com/oauth/api_scope"

# Category mapping: eBay category IDs for electronics
# https://www.ebay.com/n/all-categories
CATEGORY_MAP: dict[str, str] = {
    "TVs": "32852",            # Televisions
    "Laptops": "177",          # Laptops & Netbooks
    "Headphones": "112529",    # Headphones
    "Smartwatches": "178893",  # Smartwatches
    "Tablets": "171485",       # Tablets & eBook Readers
}

# Search keywords per category — brand-specific to find matchable products
# Generic searches return cheap no-name products that never match Best Buy inventory
CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "TVs": [
        "Samsung QLED 4K Smart TV",
        "LG OLED 4K Smart TV",
        "Sony BRAVIA 4K Smart TV",
        "TCL QLED 4K Smart TV",
        "Hisense Mini-LED Smart TV",
    ],
    "Laptops": [
        "Apple MacBook Air M3",
        "Apple MacBook Pro M4",
        "Dell XPS laptop",
        "HP OMEN gaming laptop",
        "Lenovo ThinkPad X1 Carbon",
        "ASUS ROG Strix gaming laptop",
    ],
    "Headphones": [
        "Sony WH-1000XM5",
        "Apple AirPods Pro",
        "Apple AirPods Max",
        "Bose QuietComfort Ultra",
        "Samsung Galaxy Buds3 Pro",
        "Sennheiser Momentum 4",
    ],
    "Smartwatches": [
        "Apple Watch Series 10",
        "Apple Watch Ultra 2",
        "Samsung Galaxy Watch7",
        "Google Pixel Watch 3",
        "Garmin Venu 3",
    ],
    "Tablets": [
        "Apple iPad Pro M4",
        "Apple iPad Air M2",
        "Samsung Galaxy Tab S10",
        "Microsoft Surface Pro",
    ],
}

MAX_ITEMS_PER_CATEGORY = 50  # Fewer items per search but more targeted
ITEMS_PER_PAGE = 50  # eBay max per page for Browse API
RATE_LIMIT_DELAY = 0.5  # seconds between requests

# Token cache
_token_cache: dict[str, Any] = {"token": None, "expires_at": 0}


def get_oauth_token() -> str:
    """
    Get eBay OAuth2 access token using client credentials flow.
    Tokens are cached for their lifetime (typically 2 hours).
    """
    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["token"]

    app_id = os.getenv("EBAY_APP_ID")
    cert_id = os.getenv("EBAY_CERT_ID")

    if not app_id or not cert_id:
        raise ValueError("EBAY_APP_ID and EBAY_CERT_ID must be set in environment")

    # Base64 encode credentials
    credentials = base64.b64encode(f"{app_id}:{cert_id}".encode()).decode()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {credentials}",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": EBAY_SCOPE,
    }

    logger.info("Requesting new eBay OAuth token")
    response = requests.post(EBAY_AUTH_URL, headers=headers, data=data, timeout=30)
    response.raise_for_status()

    token_data = response.json()
    _token_cache["token"] = token_data["access_token"]
    _token_cache["expires_at"] = now + token_data.get("expires_in", 7200)

    logger.info("eBay OAuth token obtained, expires in %d seconds",
                token_data.get("expires_in", 7200))
    return _token_cache["token"]


def search_category(
    category_name: str,
    category_id: str,
    keywords: str,
    max_items: int = MAX_ITEMS_PER_CATEGORY,
) -> list[dict[str, Any]]:
    """
    Search eBay Browse API for items in a category.

    Args:
        category_name: Human-readable category name (e.g., "TVs")
        category_id: eBay category ID
        keywords: Search keywords
        max_items: Maximum items to fetch

    Returns:
        List of raw eBay item dicts
    """
    token = get_oauth_token()
    all_items: list[dict[str, Any]] = []
    offset = 0

    while len(all_items) < max_items:
        headers = {
            "Authorization": f"Bearer {token}",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
            "X-EBAY-C-ENDUSERCTX": "affiliateCampaignId=<ePNCampaignId>,affiliateReferenceId=<referenceId>",
        }
        params = {
            "q": keywords,
            "category_ids": category_id,
            "limit": min(ITEMS_PER_PAGE, max_items - len(all_items)),
            "offset": offset,
            "filter": "conditions:{NEW}",  # Focus on new items for price comparison
            "sort": "price",
        }

        try:
            response = requests.get(
                EBAY_SEARCH_URL,
                headers=headers,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                logger.warning("Rate limited by eBay — waiting 10 seconds")
                time.sleep(10)
                continue
            logger.error("eBay API error for %s (offset=%d): %s",
                         category_name, offset, e)
            break

        except requests.exceptions.RequestException as e:
            logger.error("Request failed for %s: %s", category_name, e)
            break

        items = data.get("itemSummaries", [])
        if not items:
            logger.info("No more items for %s at offset %d", category_name, offset)
            break

        all_items.extend(items)
        offset += len(items)

        total_available = data.get("total", 0)
        logger.info("Fetched %d/%d items for %s (available: %d)",
                     len(all_items), max_items, category_name, total_available)

        if offset >= total_available:
            break

        # Rate limiting
        time.sleep(RATE_LIMIT_DELAY)

    return all_items


def parse_ebay_item(item: dict[str, Any], category: str) -> dict[str, Any]:
    """
    Parse a raw eBay item into the raw_listings schema.

    Maps eBay fields to the handoff contract schema:
    - title -> product_name
    - price.value -> price
    - source = "ebay"
    - itemWebUrl -> url
    """
    price_info = item.get("price", {})
    price_value = float(price_info.get("value", 0))

    # eBay doesn't have explicit sale_price in Browse API
    # We can infer from marketingPrice if present
    marketing_price = item.get("marketingPrice", {})
    original_price = None
    on_sale = False
    if marketing_price:
        original_value = marketing_price.get("originalPrice", {}).get("value")
        if original_value:
            original_price = float(original_value)
            on_sale = price_value < original_price

    # Extract brand from title heuristics (LLM matcher will do better later)
    title = item.get("title", "")

    return {
        "product_name": title,
        "price": original_price if original_price else price_value,
        "sale_price": price_value if on_sale else None,
        "on_sale": on_sale,
        "source": "ebay",
        "url": item.get("itemWebUrl", ""),
        "category": category,
        "brand": _extract_brand(title),
        "sku": item.get("itemId", ""),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "raw_payload": item,
    }


def _extract_brand(title: str) -> str:
    """Simple heuristic brand extraction from product title."""
    known_brands = [
        "Samsung", "LG", "Sony", "Apple", "Dell", "HP", "Lenovo", "ASUS",
        "Acer", "Microsoft", "Bose", "JBL", "Beats", "Sennheiser",
        "TCL", "Hisense", "Vizio", "Google", "Garmin", "Fitbit",
        "Amazon", "Panasonic", "Toshiba", "Philips",
    ]
    title_upper = title.upper()
    for brand in known_brands:
        if brand.upper() in title_upper:
            return brand
    # Fallback: first word of title
    return title.split()[0] if title else "Unknown"


def save_to_s3(all_listings: list[dict[str, Any]]) -> str | None:
    """Save raw JSON to S3 at raw/ebay/{iso_timestamp}.json."""
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        logger.warning("S3_BUCKET_NAME not set — skipping S3 upload")
        return None

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    key = f"raw/ebay/{timestamp}.json"

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
            Body=json.dumps(all_listings, indent=2, default=str),
            ContentType="application/json",
        )
        logger.info("Uploaded %d listings to s3://%s/%s", len(all_listings), bucket, key)
        return f"s3://{bucket}/{key}"

    except Exception:
        logger.exception("Failed to upload to S3")
        return None


def save_to_postgres(listings: list[dict[str, Any]]) -> int:
    """Insert parsed listings into PostgreSQL raw_listings table."""
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
                        "raw_payload": json.dumps(listing["raw_payload"], default=str),
                    },
                )
                inserted += 1

        logger.info("Inserted %d listings into PostgreSQL raw_listings", inserted)
        return inserted

    except Exception:
        logger.exception("Failed to insert into PostgreSQL")
        return 0


def main(max_items_per_keyword: int = MAX_ITEMS_PER_CATEGORY) -> None:
    """
    Main entry point: fetch from all eBay categories using brand-specific
    searches, save to S3 + Postgres.
    """
    logger.info("Starting eBay ingestion (brand-specific search)")

    all_raw_items: list[dict[str, Any]] = []
    all_parsed_listings: list[dict[str, Any]] = []

    for category_name, category_id in CATEGORY_MAP.items():
        keyword_list = CATEGORY_KEYWORDS[category_name]
        category_items: list[dict[str, Any]] = []

        for keywords in keyword_list:
            logger.info("Fetching %s — '%s' (category_id=%s)",
                         category_name, keywords, category_id)

            raw_items = search_category(
                category_name=category_name,
                category_id=category_id,
                keywords=keywords,
                max_items=max_items_per_keyword,
            )
            all_raw_items.extend(raw_items)

            parsed = [parse_ebay_item(item, category_name) for item in raw_items]
            category_items.extend(parsed)

        all_parsed_listings.extend(category_items)
        logger.info("Fetched %d total items for %s (%d brand searches)",
                     len(category_items), category_name, len(keyword_list))

    # Save raw JSON to S3
    s3_path = save_to_s3(all_raw_items)
    if s3_path:
        logger.info("S3 upload complete: %s", s3_path)

    # Save parsed listings to PostgreSQL
    count = save_to_postgres(all_parsed_listings)
    logger.info("PostgreSQL insert complete: %d rows", count)

    # Save sample fixtures for Punith's testing
    local_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "tests", "fixtures", "sample_ebay.json",
    )
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "w") as f:
        json.dump(all_parsed_listings[:10], f, indent=2, default=str)
    logger.info("Saved sample fixtures to %s", local_path)

    logger.info("eBay ingestion complete — %d total listings across %d categories",
                len(all_parsed_listings), len(CATEGORY_MAP))


if __name__ == "__main__":
    main()
