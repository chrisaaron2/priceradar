-- One row per scraped listing, cleaned and typed.
with source_data as (
    select * from {{ source('staging', 'raw_listings') }}
),

renamed as (
    select
        id as listing_id,
        product_name,
        cast(source as string) as marketplace_source,
        category,
        brand,
        sku,
        url as listing_url,
        cast(price as float64) as price,
        cast(sale_price as float64) as sale_price,
        coalesce(on_sale, false) as on_sale,
        scraped_at,
        json_value(raw_payload, '$.modelNumber') as model_number,
        safe_cast(json_value(raw_payload, '$.onlineAvailability') as bool) as in_stock,
        coalesce(json_value(raw_payload, '$._synthetic') = 'true', false) as is_synthetic
    from source_data
    where source in ('ebay', 'bestbuy')
      and price > 0
)

select
    *,
    -- What a shopper actually pays right now.
    case
        when on_sale and sale_price is not null and sale_price < price then sale_price
        else price
    end as effective_price
from renamed
