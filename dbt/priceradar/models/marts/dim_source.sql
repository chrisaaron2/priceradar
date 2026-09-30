-- One row per marketplace.
with sources as (
    select
        marketplace_source,
        logical_or(is_synthetic) as has_synthetic_data
    from {{ ref('stg_listings') }}
    group by marketplace_source
)

select
    {{ dbt_utils.generate_surrogate_key(['marketplace_source']) }} as source_key,
    marketplace_source as marketplace_name,
    case marketplace_source
        when 'ebay' then 'eBay'
        when 'bestbuy' then 'Best Buy'
        else marketplace_source
    end as display_name,
    case marketplace_source
        when 'ebay' then 'https://www.ebay.com'
        when 'bestbuy' then 'https://www.bestbuy.com'
    end as base_url,
    has_synthetic_data
from sources
