-- One row per listing observation: which product, from which marketplace, when, at what price.

with listings as (
    select * from {{ ref('stg_listings') }}
),

mapping as (
    select * from {{ ref('int_listing_products') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['l.listing_id']) }} as snapshot_id,
    l.listing_id,
    m.product_key,
    s.source_key,
    t.time_key,
    c.category_key,
    l.price,
    l.sale_price,
    l.effective_price,
    l.on_sale as was_on_sale,
    case
        when l.on_sale and l.sale_price is not null and l.sale_price < l.price
            then round((l.price - l.sale_price) / l.price * 100, 2)
        else 0.0
    end as discount_pct,
    l.in_stock,
    l.listing_url,
    l.is_synthetic
from listings as l
inner join mapping as m
    on l.marketplace_source = m.marketplace_source
    and l.product_name = m.product_name
inner join {{ ref('dim_source') }} as s on l.marketplace_source = s.marketplace_name
inner join {{ ref('dim_time') }} as t on l.scraped_at = t.scraped_at
left join {{ ref('dim_category') }} as c on l.category = c.category_name
