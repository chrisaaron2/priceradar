with source as (
    select * from {{ source('staging', 'raw_matched_products') }}
),

renamed as (
    select
        id as match_id,
        ebay_listing_id,
        bestbuy_listing_id,
        canonical_name,
        brand,
        model_number,
        confidence_score,
        is_match,
        matched_at
    from source
)

select * from renamed