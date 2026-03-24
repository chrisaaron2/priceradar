with sources as (
    select distinct
        marketplace_source
    from {{ ref('stg_listings') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['marketplace_source']) }} as source_key,
        marketplace_source as marketplace_name,
        cast(null as string) as listing_url,
        cast(null as float64) as seller_rating
    from sources
)

select * from final