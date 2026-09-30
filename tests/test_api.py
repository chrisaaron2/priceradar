import pytest

pytest.importorskip("fastapi")
pytest.importorskip("google.cloud.bigquery")

from datetime import datetime, timezone  # noqa: E402

from fastapi.testclient import TestClient  # noqa: E402

import api.main as api  # noqa: E402

PRODUCT = {
    "product_key": "abc",
    "canonical_name": "Sony WH-1000XM5",
    "brand": "Sony",
    "category": "Headphones",
    "model_number": "WH1000XM5",
    "source_count": 2,
    "is_cross_marketplace": True,
}
NOW = datetime(2026, 3, 24, tzinfo=timezone.utc)
OFFERS = [
    {
        "marketplace": "ebay",
        "display_name": "eBay",
        "price": 330.0,
        "effective_price": 299.0,
        "on_sale": True,
        "listing_url": "https://ebay.com/itm/1",
        "seen_at": NOW,
    },
    {
        "marketplace": "bestbuy",
        "display_name": "Best Buy",
        "price": 399.99,
        "effective_price": 349.99,
        "on_sale": True,
        "listing_url": "https://bestbuy.com/x",
        "seen_at": NOW,
    },
]


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(api, "table", lambda name: f"`p.d.{name}`")
    monkeypatch.delenv("API_KEY", raising=False)
    return TestClient(api.app)


def test_product_detail_orders_offers_and_computes_savings(client, monkeypatch):
    responses = iter([[PRODUCT], OFFERS])
    monkeypatch.setattr(api, "run_query", lambda sql, params=None: next(responses))
    body = client.get("/products/abc").json()
    assert body["best_source"] == "eBay"
    assert body["best_price"] == 299.0
    assert body["savings_vs_highest"] == 50.99
    assert [o["marketplace"] for o in body["offers"]] == ["ebay", "bestbuy"]


def test_unknown_product_is_404(client, monkeypatch):
    monkeypatch.setattr(api, "run_query", lambda sql, params=None: [])
    assert client.get("/products/missing").status_code == 404


def test_search_passes_filters_as_query_parameters(client, monkeypatch):
    captured = {}

    def fake(sql, params=None):
        captured.update(params)
        return [PRODUCT]

    monkeypatch.setattr(api, "run_query", fake)
    response = client.get(
        "/products", params={"q": "sony", "cross_marketplace_only": True, "limit": 5}
    )
    assert response.status_code == 200
    assert captured["q"] == ("STRING", "sony")
    assert captured["cross_only"] == ("BOOL", True)
    assert captured["limit"] == ("INT64", 5)


def test_api_key_is_enforced_when_configured(client, monkeypatch):
    monkeypatch.setenv("API_KEY", "secret")
    monkeypatch.setattr(api, "run_query", lambda sql, params=None: [PRODUCT])
    assert client.get("/products").status_code == 401
    assert client.get("/products", headers={"X-API-Key": "secret"}).status_code == 200


def test_track_product_validates_input(client):
    assert (
        client.post("/track-product", json={"product_name": "", "category": "TVs"}).status_code
        == 422
    )
