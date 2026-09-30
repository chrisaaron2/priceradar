"""
PriceRadar - eBay Browse API ingestion.

Searches the eBay Browse API across five electronics categories with brand-specific
keywords, archives the raw responses to S3 and inserts parsed rows into
Postgres `raw_listings`.

eBay data is used for inference/classification only. Nothing here is used to train or
fine-tune a model, in line with eBay's API License Agreement.
"""

from __future__ import annotations

import base64
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from common.db import insert_listings
from common.log import get_logger
from common.storage import upload_json

load_dotenv()
logger = get_logger("ebay_ingest")

EBAY_AUTH_URL = "https://api.ebay.com/identity/v1/oauth2/token"
EBAY_SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_SCOPE = "https://api.ebay.com/oauth/api_scope"

# eBay leaf category IDs for each PriceRadar category.
CATEGORY_MAP: dict[str, str] = {
    "TVs": "32852",
    "Laptops": "177",
    "Headphones": "112529",
    "Smartwatches": "178893",
    "Tablets": "171485",
}

# Brand-specific searches. Generic searches return mostly no-name products that never
# match anything sold at Best Buy.
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

KNOWN_BRANDS = [
    "Samsung",
    "LG",
    "Sony",
    "Apple",
    "Dell",
    "HP",
    "Lenovo",
    "ASUS",
    "Acer",
    "Microsoft",
    "Bose",
    "JBL",
    "Beats",
    "Sennheiser",
    "TCL",
    "Hisense",
    "Vizio",
    "Google",
    "Garmin",
    "Fitbit",
    "Amazon",
    "Panasonic",
    "Toshiba",
    "Philips",
]

MAX_ITEMS_PER_KEYWORD = int(os.getenv("EBAY_MAX_ITEMS_PER_KEYWORD", "50"))
ITEMS_PER_PAGE = 50  # Browse API maximum is 200; 50 keeps responses small
REQUEST_DELAY_S = 0.5
MAX_RETRIES = 4
REQUEST_TIMEOUT_S = 30

_token_cache: dict[str, Any] = {"token": None, "expires_at": 0.0}


def get_oauth_token() -> str:
    """Client-credentials OAuth2 token, cached until one minute before expiry."""
    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["token"]

    app_id = os.getenv("EBAY_APP_ID")
    cert_id = os.getenv("EBAY_CERT_ID")
    if not app_id or not cert_id:
        raise ValueError("EBAY_APP_ID and EBAY_CERT_ID must be set")

    credentials = base64.b64encode(f"{app_id}:{cert_id}".encode()).decode()
    response = requests.post(
        EBAY_AUTH_URL,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {credentials}",
        },
        data={"grant_type": "client_credentials", "scope": EBAY_SCOPE},
        timeout=REQUEST_TIMEOUT_S,
    )
    response.raise_for_status()
    token_data = response.json()

    _token_cache["token"] = token_data["access_token"]
    _token_cache["expires_at"] = now + token_data.get("expires_in", 7200)
    logger.info("Obtained eBay OAuth token (expires in %ss)", token_data.get("expires_in", 7200))
    return _token_cache["token"]


def _get_with_retry(params: dict[str, Any]) -> dict[str, Any] | None:
    """GET the search endpoint, backing off on 429/5xx. Returns None after MAX_RETRIES."""
    for attempt in range(1, MAX_RETRIES + 1):
        headers = {
            "Authorization": f"Bearer {get_oauth_token()}",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_US",
        }
        try:
            response = requests.get(
                EBAY_SEARCH_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT_S
            )
        except requests.RequestException as exc:
            logger.warning("eBay request failed (attempt %d/%d): %s", attempt, MAX_RETRIES, exc)
        else:
            if response.status_code == 200:
                return response.json()
            if response.status_code == 401:
                _token_cache["token"] = None  # token revoked or expired early
            elif response.status_code not in (429, 500, 502, 503, 504):
                logger.error("eBay API error %s: %s", response.status_code, response.text[:300])
                return None
            logger.warning(
                "eBay returned %s (attempt %d/%d)", response.status_code, attempt, MAX_RETRIES
            )
        time.sleep(min(2**attempt, 30))
    logger.error("Giving up on eBay search after %d attempts: %s", MAX_RETRIES, params.get("q"))
    return None


def search_category(
    category_name: str,
    category_id: str,
    keywords: str,
    max_items: int = MAX_ITEMS_PER_KEYWORD,
) -> list[dict[str, Any]]:
    """Page through search results for one keyword in one category (new items only)."""
    items: list[dict[str, Any]] = []
    offset = 0
    while len(items) < max_items:
        data = _get_with_retry(
            {
                "q": keywords,
                "category_ids": category_id,
                "limit": min(ITEMS_PER_PAGE, max_items - len(items)),
                "offset": offset,
                "filter": "conditions:{NEW}",
            }
        )
        if not data:
            break
        page = data.get("itemSummaries", [])
        if not page:
            break
        items.extend(page)
        offset += len(page)
        if offset >= data.get("total", 0):
            break
        time.sleep(REQUEST_DELAY_S)

    logger.info("%s / '%s': %d items", category_name, keywords, len(items))
    return items


def extract_brand(title: str) -> str:
    """Return the known brand that appears earliest in the title (whole words only).

    Falls back to the title's first word. Whole-word matching avoids false hits such as
    "HP" inside "HEADPHONES".
    """
    best: tuple[int, str] | None = None
    for brand in KNOWN_BRANDS:
        match = re.search(rf"\b{re.escape(brand)}\b", title, flags=re.IGNORECASE)
        if match and (best is None or match.start() < best[0]):
            best = (match.start(), brand)
    if best:
        return best[1]
    return title.split()[0] if title else "Unknown"


def _money(value: Any) -> float | None:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return None
    return amount if amount > 0 else None


def parse_ebay_item(item: dict[str, Any], category: str) -> dict[str, Any] | None:
    """Map an eBay item summary onto the raw_listings schema.

    `price` is the regular price. When eBay reports a higher original price
    (`marketingPrice.originalPrice`), the current price becomes `sale_price`.
    Items without a usable price are dropped.
    """
    current = _money(item.get("price", {}).get("value"))
    if current is None:
        return None
    original = _money(item.get("marketingPrice", {}).get("originalPrice", {}).get("value"))
    on_sale = original is not None and current < original

    title = item.get("title", "").strip()
    return {
        "product_name": title,
        "price": original if on_sale else current,
        "sale_price": current if on_sale else None,
        "on_sale": on_sale,
        "source": "ebay",
        "url": item.get("itemWebUrl", ""),
        "category": category,
        "brand": extract_brand(title),
        "sku": item.get("itemId", ""),
        "scraped_at": datetime.now(timezone.utc),
        "raw_payload": item,
    }


def main(max_items_per_keyword: int = MAX_ITEMS_PER_KEYWORD, write_fixtures: bool = False) -> int:
    """Fetch all categories, archive raw JSON to S3 and insert into Postgres.

    Returns the number of rows inserted.
    """
    raw_items: list[dict[str, Any]] = []
    listings: list[dict[str, Any]] = []

    for category, category_id in CATEGORY_MAP.items():
        for keywords in CATEGORY_KEYWORDS[category]:
            found = search_category(category, category_id, keywords, max_items_per_keyword)
            raw_items.extend(found)
            listings.extend(p for p in (parse_ebay_item(i, category) for i in found) if p)

    # The same item can come back from two keyword searches.
    unique = {listing["sku"]: listing for listing in listings}
    listings = list(unique.values())

    upload_json(raw_items, "raw/ebay")
    inserted = insert_listings(listings)
    logger.info("eBay ingestion complete: %d listings inserted", inserted)

    if write_fixtures:
        path = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "sample_ebay.json"
        path.write_text(json.dumps(listings[:10], indent=2, default=str))
        logger.info("Wrote sample fixtures to %s", path)

    return inserted


if __name__ == "__main__":
    main(write_fixtures=True)
