from llm.models import ProductPair
from llm.sku_matcher import (
    Listing,
    build_user_message,
    candidate_score,
    looks_like_accessory,
    model_tokens,
    normalize_title,
    shortlist,
)

BB_XM5 = Listing(
    1,
    "Sony - WH-1000XM5 Wireless Noise Canceling Over-the-Ear Headphones",
    399.99,
    "Headphones",
    "Sony",
)
BB_BOSE = Listing(
    2,
    "Bose - QuietComfort Ultra Wireless Noise Cancelling Over-the-Ear Headphones",
    429.0,
    "Headphones",
    "Bose",
)
BB_TV = Listing(3, 'Sony - 65" Class BRAVIA 7 LED 4K Smart TV', 1299.99, "TVs", "Sony")


def ebay(title, category="Headphones"):
    return Listing(100, title, 300.0, category, None)


def test_normalize_title_joins_model_numbers_and_drops_noise():
    assert normalize_title("NEW Sony WH-1000XM5 Headphones - SEALED") == "sony wh1000xm5 headphones"


def test_model_tokens_ignore_plain_specs():
    assert model_tokens("Samsung QN65QN85D 65 inch 144Hz 128GB") == {"qn65qn85d"}


def test_accessories_and_packaging_are_detected():
    assert looks_like_accessory("Silicone Case for AirPods Pro 2")
    assert looks_like_accessory("AirPods Pro 2nd Gen - Box Only")
    assert not looks_like_accessory("Apple AirPods Pro 2nd Gen MagSafe Charging Case USB-C")
    assert not looks_like_accessory("Microsoft Surface Go for Business 64GB")


def test_different_brand_is_never_a_candidate():
    assert candidate_score("Bose QuietComfort Ultra Headphones", BB_XM5.title, "Sony") == -1.0


def test_no_name_listing_is_never_a_candidate():
    assert (
        candidate_score("Wireless Bluetooth Headphones Noise Cancelling", BB_XM5.title, "Sony")
        == -1.0
    )


def test_product_line_implies_brand():
    score = candidate_score(
        "iPad Air 13 inch M2 128GB WiFi", "Apple - iPad Air 13-inch M2 128GB", "Apple"
    )
    assert score > 0.5


def test_shared_model_number_ranks_first():
    candidates = shortlist(
        ebay("Sony WH1000XM5 Over Ear Bluetooth Headphones Black"), [BB_BOSE, BB_XM5, BB_TV], set()
    )
    assert [c.id for _, c in candidates] == [1]


def test_shortlist_skips_judged_pairs_and_other_categories():
    listing = ebay("Sony WH-1000XM5 Headphones")
    assert shortlist(listing, [BB_XM5], {(listing.title, BB_XM5.title)}) == []
    assert shortlist(ebay("Sony WH-1000XM5 Headphones", category="TVs"), [BB_XM5], set()) == []


def test_prompt_includes_both_titles_and_prices():
    pair = ProductPair(
        ebay_title="A", bestbuy_title="B", ebay_price=10.0, bestbuy_price=None, category="TVs"
    )
    message = build_user_message(pair)
    assert "Title: A" in message and "Title: B" in message
    assert "$10.00" in message


class _FakeError(Exception):
    def __init__(self, status_code):
        super().__init__(f"error {status_code}")
        self.status_code = status_code


class _FakeClient:
    def __init__(self, error):
        self.error = error
        self.calls = 0
        self.chat = self
        self.completions = self

    def create(self, **kwargs):
        self.calls += 1
        raise self.error


def test_unknown_model_stops_the_run_instead_of_burning_the_budget():
    import pytest

    from llm.sku_matcher import MatcherConfigError, match_pair_with_llm

    client = _FakeClient(_FakeError(404))
    pair = ProductPair(ebay_title="A", bestbuy_title="B", category="TVs")
    with pytest.raises(MatcherConfigError):
        match_pair_with_llm(client, pair)
    assert client.calls == 1


def test_other_api_errors_skip_just_that_pair():
    from llm.sku_matcher import match_pair_with_llm

    client = _FakeClient(_FakeError(500))
    pair = ProductPair(ebay_title="A", bestbuy_title="B", category="TVs")
    assert match_pair_with_llm(client, pair) is None
