"""
PriceRadar - Pydantic Models for LLM SKU Matching

Defines the structured output schemas that the LLM returns when
comparing product pairs across marketplaces.

Owner: Chris (Track A)
"""

from typing import Optional
from pydantic import BaseModel, Field


class ProductPair(BaseModel):
    """Input: A pair of products from different marketplaces to compare."""
    ebay_title: str = Field(description="Product title from eBay listing")
    bestbuy_title: str = Field(description="Product title from Best Buy listing")
    ebay_price: Optional[float] = Field(default=None, description="eBay listing price")
    bestbuy_price: Optional[float] = Field(default=None, description="Best Buy listing price")
    category: str = Field(description="Product category (TVs, Laptops, etc.)")
    ebay_listing_id: Optional[int] = Field(default=None, description="raw_listings.id for eBay product")
    bestbuy_listing_id: Optional[int] = Field(default=None, description="raw_listings.id for Best Buy product")


class SKUMatch(BaseModel):
    """
    Output: LLM's determination of whether two products are the same.

    This is the structured output schema sent to Claude via tool_use.
    The LLM fills in all fields based on comparing the product pair.
    """
    is_match: bool = Field(
        description="Whether the two products are the same item (True/False)"
    )
    confidence: float = Field(
        ge=0.0, le=1.0,
        description="Confidence score from 0.0 (no confidence) to 1.0 (certain match)"
    )
    canonical_name: Optional[str] = Field(
        default="Unknown",
        description="Standardized product name combining info from both sources. "
                    "Format: 'Brand ModelNumber ScreenSize Type' e.g. 'Samsung QN65Q80C 65\" QLED 4K Smart TV'"
    )
    brand: Optional[str] = Field(
        default="Unknown",
        description="Extracted brand name, normalized (e.g. 'Samsung' not 'SAMSUNG')"
    )
    model_number: Optional[str] = Field(
        default=None,
        description="Extracted model number if identifiable (e.g. 'QN65Q80C', 'WH1000XM5')"
    )
    reasoning: str = Field(
        description="Brief explanation of why the LLM thinks they match or don't match. "
                    "Should reference specific identifiers like model numbers, specs, or brand."
    )
