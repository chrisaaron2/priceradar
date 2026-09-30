from ingestion.ebay_ingest import extract_brand, parse_ebay_item


def _item(**overrides):
    item = {
        "itemId": "v1|123|0",
        "title": "Sony WH-1000XM5 Wireless Noise Canceling Headphones Black",
        "price": {"value": "298.00", "currency": "USD"},
        "itemWebUrl": "https://www.ebay.com/itm/123",
    }
    item.update(overrides)
    return item


def test_regular_price_listing():
    row = parse_ebay_item(_item(), "Headphones")
    assert row["price"] == 298.0
    assert row["sale_price"] is None
    assert row["on_sale"] is False
    assert row["source"] == "ebay"
    assert row["brand"] == "Sony"
    assert row["sku"] == "v1|123|0"


def test_discounted_listing_uses_original_as_price():
    row = parse_ebay_item(
        _item(marketingPrice={"originalPrice": {"value": "399.99"}}), "Headphones"
    )
    assert row["price"] == 399.99
    assert row["sale_price"] == 298.0
    assert row["on_sale"] is True


def test_listing_without_price_is_dropped():
    assert parse_ebay_item(_item(price={}), "Headphones") is None
    assert parse_ebay_item(_item(price={"value": "0"}), "Headphones") is None


def test_brand_is_whole_word_and_earliest():
    # "HP" must not match inside "Headphones"
    assert extract_brand("Sennheiser Momentum 4 Wireless Headphones") == "Sennheiser"
    assert extract_brand("OtterBox case for Samsung Galaxy Tab S10") == "Samsung"
    assert extract_brand("hp OMEN 16 gaming laptop") == "HP"
    assert extract_brand("Generic Smart Watch") == "Generic"
