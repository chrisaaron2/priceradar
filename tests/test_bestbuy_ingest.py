from collections import Counter

from ingestion.bestbuy_ingest import PRODUCT_CATALOG, generate_listings

REQUIRED = {
    "product_name",
    "price",
    "sale_price",
    "on_sale",
    "source",
    "url",
    "category",
    "brand",
    "sku",
    "scraped_at",
    "raw_payload",
}


def test_listings_match_raw_listings_schema():
    listings = generate_listings(products_per_category=5, seed=1)
    assert len(listings) == 5 * len(PRODUCT_CATALOG)
    for row in listings:
        assert REQUIRED <= row.keys()
        assert row["source"] == "bestbuy"
        assert row["price"] > 0
        assert row["raw_payload"]["_synthetic"] is True
        if row["on_sale"]:
            assert row["sale_price"] < row["price"]
        else:
            assert row["sale_price"] is None


def test_no_duplicate_products_within_a_run():
    listings = generate_listings(products_per_category=10, seed=2)
    counts = Counter((row["category"], row["product_name"]) for row in listings)
    assert max(counts.values()) == 1


def test_seed_makes_output_reproducible():
    a = generate_listings(products_per_category=3, seed=7)
    b = generate_listings(products_per_category=3, seed=7)
    assert [r["price"] for r in a] == [r["price"] for r in b]
