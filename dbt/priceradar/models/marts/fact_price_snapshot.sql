with listings as (
    select * from {{ ref('stg_listings') }}
),

dim_product as (
    select * from {{ ref('dim_product') }}
),

dim_source as (
    select * from {{ ref('dim_source') }}
),

dim_time as (
    select * from {{ ref('dim_time') }}
),

dim_category as (
    select * from {{ ref('dim_category') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['l.listing_id', 'l.scraped_at']) }} as snapshot_id,
        p.product_key,
        s.source_key,
        t.time_key,
        c.category_key,
        l.price,
        l.on_sale as was_on_sale,
        case
            when l.sale_price is not null and l.price > 0
            then round((l.price - l.sale_price) / l.price * 100, 2)
            else 0.0
        end as discount_pct,
        true as in_stock
    from listings l
    left join dim_product p on l.product_name = p.canonical_name
    left join dim_source s on l.marketplace_source = s.marketplace_name
    left join dim_time t on l.scraped_at = t.scraped_at
    left join dim_category c on l.category = c.category_name
)

select * from final