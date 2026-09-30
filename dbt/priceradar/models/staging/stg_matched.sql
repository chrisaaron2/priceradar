-- One row per pair of listings judged by the LLM matcher.
with source_data as (
    select * from {{ source('staging', 'raw_matched_products') }}
)

select
    id as match_id,
    ebay_listing_id,
    bestbuy_listing_id,
    canonical_name,
    brand,
    model_number,
    cast(confidence_score as float64) as confidence_score,
    is_match,
    matched_at,
    is_match and confidence_score >= {{ var('match_confidence_threshold') }} as is_accepted
from source_data
