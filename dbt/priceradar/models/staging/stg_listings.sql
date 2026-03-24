with source_data as (
    select * from {{ source('staging', 'raw_listings') }}
),

renamed as (
    select
        id as listing_id,
        product_name,
        cast(price as float64) as price,
        cast(sale_price as float64) as sale_price,
        on_sale,
        cast(source as string) as marketplace_source,
        url,
        category,
        brand,
        sku,
        scraped_at,
        raw_payload
    from source_data
)

select * from renamed