"""
PriceRadar - LLM SKU Matching Layer (Groq + Llama 3.3 70B)

Uses Groq's ultra-fast inference API with Llama 3.3 70B to determine
whether products from eBay and Best Buy are the same item.

Why Groq: Runs Llama 3.3 70B on custom LPU hardware with millisecond
latency. Free tier allows 30 RPM — enough to process all pairs in
a single run. No cloud API costs.

This is INFERENCE/CLASSIFICATION only — no eBay data is used to train
or fine-tune any AI model. This complies with eBay's June 2025 API
License Agreement.

Owner: Chris (Track A)
"""

import json
import logging
import os
import time
from difflib import SequenceMatcher
from typing import Any

from groq import Groq
from pydantic import ValidationError
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from llm.models import ProductPair, SKUMatch

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("sku_matcher")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
MODEL = "llama-3.3-70b-versatile"  # Best quality on Groq free tier
MAX_PAIRS_PER_RUN = 50
MAX_CANDIDATES = 5
MIN_SIMILARITY = 0.25
CONFIDENCE_THRESHOLD = 0.6

# Rate limiting — Groq free tier: 30 RPM
REQUEST_DELAY = 7.0  # ~8 RPM, stays under 12K TPM limit

# PostgreSQL connection
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "priceradar")
PG_USER = os.getenv("POSTGRES_USER", "priceradar")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "priceradar_dev")
PG_URL = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# System prompt
SYSTEM_PROMPT = """You are a product matching specialist for a competitive pricing intelligence platform.

Your job is to determine whether two product listings from different marketplaces (eBay and Best Buy) 
refer to the SAME physical product.

Guidelines:
- Focus on: brand, model number, screen size, storage capacity, color, generation/year
- Model numbers are the strongest signal (e.g., QN65Q80C, WH1000XM5, OLED65C4PUA)
- Different sellers listing the same product may use different title formats
- Ignore differences in: seller name, shipping info, bundle accessories, listing format
- "New" vs "Open Box" condition differences do NOT make them different products
- If one title has a model number and the other doesn't, but specs match, it can still be a match
- Be conservative: when in doubt, set is_match=false with lower confidence

Confidence scoring:
- 0.9-1.0: Model numbers match exactly
- 0.7-0.89: Strong spec match (brand + size + type) but model number not confirmed
- 0.5-0.69: Partial match, some specs align but significant uncertainty
- 0.0-0.49: Likely different products

You MUST respond with ONLY valid JSON matching this exact schema, no other text:
{
    "is_match": true or false,
    "confidence": 0.0 to 1.0,
    "canonical_name": "Standardized product name",
    "brand": "Brand name",
    "model_number": "Model number or null",
    "reasoning": "Brief explanation"
}"""


def get_engine():
    """Create SQLAlchemy engine."""
    return create_engine(PG_URL)


def get_products_by_category(engine) -> dict[str, dict[str, list[dict]]]:
    """Fetch products from raw_listings grouped by category and source."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, product_name, price, sale_price, source, category, brand, sku
            FROM raw_listings
            ORDER BY category, source
        """))
        rows = result.fetchall()

    categories: dict[str, dict[str, list[dict]]] = {}
    for row in rows:
        cat = row[5]
        source = row[4]
        if cat not in categories:
            categories[cat] = {"ebay": [], "bestbuy": []}
        product = {
            "id": row[0],
            "product_name": row[1],
            "price": float(row[2]) if row[2] else None,
            "sale_price": float(row[3]) if row[3] else None,
            "source": source,
            "category": cat,
            "brand": row[6],
            "sku": row[7],
        }
        if source in categories[cat]:
            categories[cat][source].append(product)

    for cat, sources in categories.items():
        logger.info("Category '%s': %d eBay, %d Best Buy products",
                     cat, len(sources["ebay"]), len(sources["bestbuy"]))

    return categories


def get_existing_matches(engine) -> set[tuple[int, int]]:
    """Get already-matched product pairs to avoid re-processing."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT ebay_listing_id, bestbuy_listing_id
            FROM matched_products
        """))
        return {(row[0], row[1]) for row in result.fetchall()}


def string_similarity(a: str, b: str) -> float:
    """Calculate string similarity ratio between two product names."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def find_candidates(
    bestbuy_product: dict,
    ebay_products: list[dict],
    existing_matches: set[tuple[int, int]],
    max_candidates: int = MAX_CANDIDATES,
) -> list[dict]:
    """Find top N eBay candidates using string similarity as pre-filter."""
    bb_name = bestbuy_product["product_name"].lower()
    bb_id = bestbuy_product["id"]

    scored = []
    for ebay_prod in ebay_products:
        if (ebay_prod["id"], bb_id) in existing_matches:
            continue
        similarity = string_similarity(bb_name, ebay_prod["product_name"])
        if similarity >= MIN_SIMILARITY:
            scored.append((similarity, ebay_prod))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [prod for _, prod in scored[:max_candidates]]


def match_pair_with_llm(client: Groq, pair: ProductPair) -> SKUMatch | None:
    """Send a product pair to Groq/Llama for structured matching."""
    ebay_price_str = f"  Price: ${pair.ebay_price:.2f}\n" if pair.ebay_price else ""
    bestbuy_price_str = f"  Price: ${pair.bestbuy_price:.2f}\n" if pair.bestbuy_price else ""

    user_message = (
        f"Compare these two product listings and determine if they are the same product:\n\n"
        f"**eBay Listing:**\n"
        f"  Title: {pair.ebay_title}\n"
        f"{ebay_price_str}"
        f"  Category: {pair.category}\n\n"
        f"**Best Buy Listing:**\n"
        f"  Title: {pair.bestbuy_title}\n"
        f"{bestbuy_price_str}"
        f"  Category: {pair.category}\n\n"
        f"Determine if these are the same physical product. "
        f"Respond with JSON only."
    )

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ],
                temperature=0.1,
                max_tokens=512,
                response_format={"type": "json_object"},
            )

            response_text = response.choices[0].message.content.strip()

            try:
                data = json.loads(response_text)
                match = SKUMatch(**data)
                return match
            except (json.JSONDecodeError, ValidationError) as e:
                logger.warning("Failed to parse response (attempt %d): %s\nRaw: %s",
                                 attempt + 1, e, response_text[:200])
                if attempt < 2:
                    continue
                return None

        except Exception as e:
            error_str = str(e)
            if "429" in error_str or "rate_limit" in error_str.lower():
                wait_time = 15 * (attempt + 1)
                logger.warning("Rate limited (attempt %d/3) — waiting %ds",
                                 attempt + 1, wait_time)
                time.sleep(wait_time)
                if attempt == 2:
                    return None
            else:
                logger.error("Groq API error: %s", e)
                return None

    return None


def save_match(engine, ebay_id: int, bestbuy_id: int, match: SKUMatch) -> None:
    """Save a match result to the matched_products table."""
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO matched_products
                    (ebay_listing_id, bestbuy_listing_id, canonical_name,
                     brand, model_number, confidence_score, is_match, matched_at)
                VALUES
                    (:ebay_id, :bestbuy_id, :canonical_name,
                     :brand, :model_number, :confidence, :is_match, NOW())
            """),
            {
                "ebay_id": ebay_id,
                "bestbuy_id": bestbuy_id,
                "canonical_name": match.canonical_name or "Unknown",
                "brand": match.brand or "Unknown",
                "model_number": match.model_number,
                "confidence": match.confidence,
                "is_match": match.is_match,
            },
        )


def main(max_pairs: int = MAX_PAIRS_PER_RUN) -> None:
    """Main entry point: match products across eBay and Best Buy."""
    if not GROQ_API_KEY:
        logger.error("GROQ_API_KEY not set — cannot run SKU matching")
        return

    logger.info("=" * 60)
    logger.info("Starting LLM SKU Matching")
    logger.info("Model: %s (via Groq)", MODEL)
    logger.info("Max pairs: %d | Delay: %.1fs", max_pairs, REQUEST_DELAY)
    logger.info("=" * 60)

    client = Groq(api_key=GROQ_API_KEY)
    engine = get_engine()

    categories = get_products_by_category(engine)
    existing_matches = get_existing_matches(engine)
    logger.info("Existing matches in DB: %d", len(existing_matches))

    pairs_processed = 0
    matches_found = 0
    non_matches = 0

    for category, sources in categories.items():
        ebay_products = sources["ebay"]
        bestbuy_products = sources["bestbuy"]

        if not ebay_products or not bestbuy_products:
            logger.info("Skipping '%s' — missing products from one source", category)
            continue

        logger.info("Processing category: %s (%d eBay x %d Best Buy)",
                     category, len(ebay_products), len(bestbuy_products))

        for bb_product in bestbuy_products:
            if pairs_processed >= max_pairs:
                break

            candidates = find_candidates(bb_product, ebay_products, existing_matches)
            if not candidates:
                continue

            for ebay_product in candidates:
                if pairs_processed >= max_pairs:
                    break

                pair = ProductPair(
                    ebay_title=ebay_product["product_name"],
                    bestbuy_title=bb_product["product_name"],
                    ebay_price=ebay_product["price"],
                    bestbuy_price=bb_product["price"],
                    category=category,
                    ebay_listing_id=ebay_product["id"],
                    bestbuy_listing_id=bb_product["id"],
                )

                logger.info("  [%d/%d] [eBay] %s",
                             pairs_processed + 1, max_pairs,
                             ebay_product["product_name"][:55])
                logger.info("          [BB]  %s",
                             bb_product["product_name"][:55])

                match = match_pair_with_llm(client, pair)

                if match:
                    save_match(engine, ebay_product["id"], bb_product["id"], match)

                    status = "MATCH" if match.is_match else "NO MATCH"
                    logger.info("    -> %s (%.2f) %s",
                                 status, match.confidence, match.reasoning[:70])

                    if match.is_match and match.confidence >= CONFIDENCE_THRESHOLD:
                        matches_found += 1
                    else:
                        non_matches += 1
                else:
                    logger.warning("    -> FAILED (no valid response)")

                pairs_processed += 1
                time.sleep(REQUEST_DELAY)

        if pairs_processed >= max_pairs:
            break

    logger.info("=" * 60)
    logger.info("SKU Matching Complete!")
    logger.info("  Pairs processed: %d", pairs_processed)
    logger.info("  Matches found:   %d", matches_found)
    logger.info("  Non-matches:     %d", non_matches)
    logger.info("  Cost: $0.00 (Groq free tier + Llama 3.3 70B)")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
