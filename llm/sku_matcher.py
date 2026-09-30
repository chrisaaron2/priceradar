"""
PriceRadar - LLM product matching (GPT-OSS 120B on Groq).

Decides which eBay listings are the same physical product as a Best Buy catalog item.

1. Work on distinct titles, not rows. The same title is scraped every run, so each
   (eBay title, Best Buy title) pair is judged once and the verdict is reused.
2. Skip eBay titles that are accessories or packaging ("case for ...", "box only").
   For each remaining eBay title that has no accepted match yet, shortlist Best Buy titles in the
   same category and brand by a cheap text score (normalized title similarity plus a
   bonus for a shared model number).
3. Ask the LLM about the most promising pairs first, stopping at the first match for
   each eBay title, until the per-run budget is spent.
4. Store every verdict in `matched_products`. dbt uses the confident matches to build
   `dim_product`.

The LLM only classifies listings. No eBay data is used for training or fine-tuning.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from difflib import SequenceMatcher

from dotenv import load_dotenv
from pydantic import ValidationError
from sqlalchemy import text

from common.db import get_engine
from common.log import get_logger
from llm.models import ProductPair, SKUMatch

load_dotenv()
logger = get_logger("sku_matcher")

# Groq retired llama-3.3-70b-versatile for free and developer tiers on 2026-08-16.
MODEL = os.getenv("MATCHER_MODEL", "openai/gpt-oss-120b")
MAX_PAIRS_PER_RUN = int(os.getenv("MATCHER_MAX_PAIRS", "50"))
MAX_CANDIDATES = int(os.getenv("MATCHER_MAX_CANDIDATES", "3"))
MIN_SCORE = float(os.getenv("MATCHER_MIN_SCORE", "0.35"))
CONFIDENCE_THRESHOLD = float(os.getenv("MATCHER_CONFIDENCE_THRESHOLD", "0.6"))
# ~8 requests/min keeps well inside Groq's free-tier request and token limits.
REQUEST_DELAY_S = float(os.getenv("MATCHER_REQUEST_DELAY", "7.0"))
MAX_ATTEMPTS = 3
# Errors that retrying cannot fix (bad key, unknown model, bad request): stop the run.
FATAL_STATUS_CODES = {400, 401, 403, 404}


class MatcherConfigError(RuntimeError):
    """The LLM provider rejected the request in a way that affects every pair."""


KNOWN_BRANDS = {
    "samsung",
    "lg",
    "sony",
    "apple",
    "dell",
    "hp",
    "lenovo",
    "asus",
    "acer",
    "microsoft",
    "bose",
    "jbl",
    "beats",
    "sennheiser",
    "tcl",
    "hisense",
    "vizio",
    "google",
    "garmin",
    "fitbit",
    "amazon",
    "panasonic",
    "toshiba",
    "philips",
}
# Product lines that imply a brand even when sellers leave the brand out.
LINE_BRANDS = {
    "airpods": "apple",
    "airpod": "apple",
    "ipad": "apple",
    "macbook": "apple",
    "galaxy": "samsung",
    "bravia": "sony",
    "thinkpad": "lenovo",
    "surface": "microsoft",
    "pixel": "google",
    "omen": "hp",
    "spectre": "hp",
    "rog": "asus",
    "zephyrus": "asus",
    "xps": "dell",
    "quietcomfort": "bose",
}
NOISE_WORDS = {
    "new",
    "brand",
    "sealed",
    "factory",
    "authentic",
    "genuine",
    "original",
    "fast",
    "free",
    "shipping",
    "ship",
    "nib",
    "bnib",
    "open",
    "box",
    "latest",
    "model",
    "the",
    "with",
    "and",
    "for",
    "in",
    "of",
    "class",
}

SYSTEM_PROMPT = """You are a product matching specialist for a price comparison platform.

Decide whether an eBay listing and a Best Buy listing refer to the SAME physical product.

Guidelines:
- Compare brand, model number, screen size, storage, memory, color, generation/year.
- Model numbers are the strongest signal (e.g. QN65Q80C, WH1000XM5, OLED65C4PUA).
- Sellers format titles differently; ignore seller names, shipping, bundles, listing style.
- New vs. open box is the same product.
- Different storage, size, generation or chip (e.g. M2 vs M3) is a different product.
- Accessories (cases, chargers, screen protectors) and "box only" or "empty box" listings
  are never the product itself.
- If one title lacks a model number but every stated spec matches, it can still match.
- Be conservative: when in doubt, is_match=false with lower confidence.

Confidence:
- 0.90-1.00: model numbers match exactly
- 0.70-0.89: brand, line and key specs match; model number not confirmed
- 0.50-0.69: partial match with real uncertainty
- 0.00-0.49: likely different products

Respond with ONLY a JSON object:
{"is_match": true|false, "confidence": 0.0-1.0, "canonical_name": "Brand Line Model Key-specs",
 "brand": "Brand", "model_number": "model or null", "reasoning": "one sentence"}"""


@dataclass(frozen=True)
class Listing:
    id: int
    title: str
    price: float | None
    category: str
    brand: str | None


# Candidate scoring (pure functions, unit-tested)
def normalize_title(title: str) -> str:
    """Lower-case, join hyphenated model numbers, drop punctuation and noise words."""
    t = title.lower().replace('"', " inch ").replace("”", " inch ")
    t = re.sub(r"(?<=[a-z0-9])-(?=[a-z0-9])", "", t)  # wh-1000xm5 -> wh1000xm5
    t = re.sub(r"[^a-z0-9. ]+", " ", t)
    return " ".join(w for w in t.split() if w not in NOISE_WORDS)


SPEC_TOKEN = re.compile(r"^\d+(\.\d+)?(gb|tb|mm|hz|in|inch|w|mah|gen|th|nd|rd|st)$")


def model_tokens(title: str) -> set[str]:
    """Tokens that look like model numbers (4+ chars mixing letters and digits).

    Plain specs such as "128gb", "45mm" or "144hz" are not model numbers.
    """
    return {
        tok
        for tok in normalize_title(title).split()
        if len(tok) >= 4
        and re.search(r"[a-z]", tok)
        and re.search(r"\d", tok)
        and not SPEC_TOKEN.match(tok)
    }


ACCESSORY_PATTERN = re.compile(
    r"\b(box only|empty box|for parts|compatible with)\b|\bfor\b(?! business\b)",
    flags=re.IGNORECASE,
)


def looks_like_accessory(title: str) -> bool:
    """True for listings that sell something *for* a product (cases, remotes, bands,
    chargers, screen protectors) or only its packaging. Such titles are skipped before
    any LLM call; on real eBay data they are close to half of all titles."""
    return bool(ACCESSORY_PATTERN.search(title))


def brands_in(title: str) -> set[str]:
    """Known brands named in a title, directly or through a product line (iPad -> apple)."""
    words = set(re.findall(r"[a-z]+", title.lower()))
    return (words & KNOWN_BRANDS) | {LINE_BRANDS[w] for w in words & LINE_BRANDS.keys()}


def candidate_score(ebay_title: str, bestbuy_title: str, bestbuy_brand: str | None) -> float:
    """Cheap relevance score, roughly 0 to 1.4; -1 means "never a match".

    A pair is only considered when the eBay title names the Best Buy brand (directly or
    via a product line such as "iPad") or both titles share a model number. The score
    blends character similarity and word overlap of the normalized titles, plus a bonus
    for a shared model number.
    """
    brand = (bestbuy_brand or "").lower()
    shared_model = bool(model_tokens(ebay_title) & model_tokens(bestbuy_title))
    if brand not in brands_in(ebay_title) and not shared_model:
        return -1.0

    a, b = normalize_title(ebay_title), normalize_title(bestbuy_title)
    ta, tb = set(a.split()), set(b.split())
    containment = len(ta & tb) / min(len(ta), len(tb)) if ta and tb else 0.0
    score = 0.5 * SequenceMatcher(None, a, b).ratio() + 0.5 * containment
    if shared_model:
        score += 0.3
    return score


def shortlist(
    ebay: Listing,
    bestbuy: list[Listing],
    judged: set[tuple[str, str]],
    max_candidates: int = MAX_CANDIDATES,
    min_score: float = MIN_SCORE,
) -> list[tuple[float, Listing]]:
    """Top Best Buy candidates for one eBay title, skipping pairs judged before."""
    if looks_like_accessory(ebay.title):
        return []
    scored = []
    for bb in bestbuy:
        if bb.category != ebay.category or (ebay.title, bb.title) in judged:
            continue
        score = candidate_score(ebay.title, bb.title, bb.brand)
        if score >= min_score:
            scored.append((score, bb))
    scored.sort(key=lambda pair: pair[0], reverse=True)
    return scored[:max_candidates]


# Database access
LATEST_TITLES_SQL = text(
    """
    SELECT DISTINCT ON (source, category, product_name)
           id, product_name, COALESCE(sale_price, price) AS price, source, category, brand
    FROM raw_listings
    WHERE source IN ('ebay', 'bestbuy') AND category IS NOT NULL
    ORDER BY source, category, product_name, scraped_at DESC, id DESC
    """
)

JUDGED_PAIRS_SQL = text(
    """
    SELECT e.product_name AS ebay_title, b.product_name AS bestbuy_title,
           m.is_match, m.confidence_score
    FROM matched_products m
    JOIN raw_listings e ON e.id = m.ebay_listing_id
    JOIN raw_listings b ON b.id = m.bestbuy_listing_id
    """
)

INSERT_MATCH_SQL = text(
    """
    INSERT INTO matched_products
        (ebay_listing_id, bestbuy_listing_id, canonical_name, brand,
         model_number, confidence_score, is_match, matched_at)
    VALUES
        (:ebay_id, :bestbuy_id, :canonical_name, :brand,
         :model_number, :confidence, :is_match, NOW() AT TIME ZONE 'UTC')
    """
)


def load_titles(engine) -> tuple[list[Listing], list[Listing]]:
    ebay, bestbuy = [], []
    with engine.connect() as conn:
        for row in conn.execute(LATEST_TITLES_SQL).mappings():
            listing = Listing(
                id=row["id"],
                title=row["product_name"],
                price=float(row["price"]) if row["price"] is not None else None,
                category=row["category"],
                brand=row["brand"],
            )
            (ebay if row["source"] == "ebay" else bestbuy).append(listing)
    logger.info("Distinct titles: %d eBay, %d Best Buy", len(ebay), len(bestbuy))
    return ebay, bestbuy


def load_judged(engine) -> tuple[set[tuple[str, str]], set[str]]:
    """Title pairs already judged, and eBay titles that already have an accepted match."""
    judged, matched = set(), set()
    with engine.connect() as conn:
        for row in conn.execute(JUDGED_PAIRS_SQL).mappings():
            judged.add((row["ebay_title"], row["bestbuy_title"]))
            if row["is_match"] and row["confidence_score"] >= CONFIDENCE_THRESHOLD:
                matched.add(row["ebay_title"])
    return judged, matched


def save_match(engine, pair: ProductPair, match: SKUMatch) -> None:
    with engine.begin() as conn:
        conn.execute(
            INSERT_MATCH_SQL,
            {
                "ebay_id": pair.ebay_listing_id,
                "bestbuy_id": pair.bestbuy_listing_id,
                "canonical_name": match.canonical_name or pair.bestbuy_title,
                "brand": match.brand or "Unknown",
                "model_number": match.model_number,
                "confidence": match.confidence,
                "is_match": match.is_match,
            },
        )


# LLM call
def build_user_message(pair: ProductPair) -> str:
    def price(p: float | None) -> str:
        return f"\n  Price: ${p:.2f}" if p else ""

    return (
        "Are these the same product?\n\n"
        f"eBay listing:\n  Title: {pair.ebay_title}{price(pair.ebay_price)}\n\n"
        f"Best Buy listing:\n  Title: {pair.bestbuy_title}{price(pair.bestbuy_price)}\n\n"
        f"Category: {pair.category}. Respond with JSON only."
    )


def _completion_kwargs() -> dict:
    kwargs = {
        "model": MODEL,
        "temperature": 0.1,
        "max_tokens": 1024,
        "response_format": {"type": "json_object"},
    }
    if MODEL.startswith("openai/gpt-oss"):
        # Reasoning models spend completion tokens thinking; keep that short.
        kwargs["extra_body"] = {"reasoning_effort": "low"}
    return kwargs


def match_pair_with_llm(client, pair: ProductPair) -> SKUMatch | None:
    """Ask the model about one pair. Retries bad JSON and rate limits; raises
    MatcherConfigError on errors that would fail every pair (bad key, unknown model)."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": build_user_message(pair)},
                ],
                **_completion_kwargs(),
            )
            content = response.choices[0].message.content or ""
            return SKUMatch(**json.loads(content))
        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning("Unparseable reply (attempt %d/%d): %s", attempt, MAX_ATTEMPTS, exc)
        except Exception as exc:  # groq raises its own error types; keep this dependency-light
            status = getattr(exc, "status_code", None)
            if status in FATAL_STATUS_CODES:
                raise MatcherConfigError(f"Groq rejected the request ({status}): {exc}") from exc
            if status == 429 or "rate" in str(exc).lower():
                wait = 15 * attempt
                logger.warning(
                    "Rate limited (attempt %d/%d); waiting %ds", attempt, MAX_ATTEMPTS, wait
                )
                time.sleep(wait)
            else:
                logger.error("Groq API error: %s", exc)
                return None
    return None


# Entry point
def main(max_pairs: int = MAX_PAIRS_PER_RUN) -> dict[str, int]:
    """Run one matching pass. Returns counts of pairs judged, matches and failures."""
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set")

    from groq import Groq

    client = Groq(api_key=api_key)
    engine = get_engine()
    ebay, bestbuy = load_titles(engine)
    judged, matched_titles = load_judged(engine)
    logger.info(
        "Already judged: %d pairs; %d eBay titles matched", len(judged), len(matched_titles)
    )

    # Plan: eBay titles without a match, most promising first.
    plan = []
    for listing in ebay:
        if listing.title in matched_titles:
            continue
        candidates = shortlist(listing, bestbuy, judged)
        if candidates:
            plan.append((candidates[0][0], listing, candidates))
    plan.sort(key=lambda item: item[0], reverse=True)
    logger.info("%d eBay titles have candidates; budget is %d LLM calls", len(plan), max_pairs)

    stats = {"judged": 0, "matches": 0, "failed": 0}
    for _, ebay_listing, candidates in plan:
        for score, bb in candidates:
            if stats["judged"] + stats["failed"] >= max_pairs:
                break
            pair = ProductPair(
                ebay_title=ebay_listing.title,
                bestbuy_title=bb.title,
                ebay_price=ebay_listing.price,
                bestbuy_price=bb.price,
                category=ebay_listing.category,
                ebay_listing_id=ebay_listing.id,
                bestbuy_listing_id=bb.id,
            )
            match = match_pair_with_llm(client, pair)
            time.sleep(REQUEST_DELAY_S)
            if match is None:
                stats["failed"] += 1
                continue

            save_match(engine, pair, match)
            stats["judged"] += 1
            accepted = match.is_match and match.confidence >= CONFIDENCE_THRESHOLD
            logger.info(
                "%s (%.2f, score %.2f) %s  <->  %s",
                "MATCH" if accepted else "no match",
                match.confidence,
                score,
                ebay_listing.title[:60],
                bb.title[:60],
            )
            if accepted:
                stats["matches"] += 1
                break  # this eBay title is resolved
        else:
            continue
        if stats["judged"] + stats["failed"] >= max_pairs:
            break

    logger.info("Matching complete: %s", stats)
    return stats


if __name__ == "__main__":
    main()
