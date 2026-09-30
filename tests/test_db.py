import json
from datetime import datetime, timezone

from common.db import _listing_params


def test_listing_params_serialize_payload_and_timestamp():
    params = _listing_params(
        {
            "product_name": "X",
            "price": 10.0,
            "source": "ebay",
            "scraped_at": datetime(2026, 3, 1, 12, tzinfo=timezone.utc),
            "raw_payload": {"a": 1},
        }
    )
    assert params["scraped_at"] == "2026-03-01T12:00:00+00:00"
    assert json.loads(params["raw_payload"]) == {"a": 1}
    assert params["on_sale"] is False
    assert params["sale_price"] is None


def test_missing_timestamp_is_left_for_the_database():
    params = _listing_params({"product_name": "X", "source": "bestbuy"})
    assert params["scraped_at"] is None
