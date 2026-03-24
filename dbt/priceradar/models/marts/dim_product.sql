with listings as (
    select distinct
        product_name,
        brand
    from {{ ref('stg_listings') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_name']) }} as product_key,
        product_name as canonical_name,
        brand,
        cast(null as string) as model_number,
        cast(null as float64) as matched_confidence_score
    from listings
)

select * from final