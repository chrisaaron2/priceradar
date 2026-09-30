-- One row per real-world product. A product matched across marketplaces appears once,
-- with every source's listings pointing at it through fact_price_snapshot.

with mapping as (
    select * from {{ ref('int_listing_products') }}
),

listings as (
    select * from {{ ref('stg_listings') }}
),

-- Latest listing of the anchor title supplies brand, category and model number.
anchor_listing as (
    select
        m.product_key,
        l.product_name,
        l.brand,
        l.category,
        l.model_number
    from mapping as m
    inner join listings as l
        on l.marketplace_source = m.anchor_source
        and l.product_name = m.anchor_title
    qualify row_number() over (partition by m.product_key order by l.scraped_at desc, l.listing_id desc) = 1
),

-- The most confident LLM verdict supplies a clean canonical name and model number.
best_match as (
    select product_key, match_canonical_name, match_model_number, match_confidence
    from mapping
    where match_confidence is not null
    qualify row_number() over (partition by product_key order by match_confidence desc) = 1
),

coverage as (
    select
        product_key,
        count(distinct marketplace_source) as source_count,
        count(distinct product_name) as listing_title_count
    from mapping
    group by product_key
)

select
    a.product_key,
    coalesce(nullif(b.match_canonical_name, 'Unknown'), a.product_name) as canonical_name,
    a.brand,
    a.category,
    coalesce(a.model_number, b.match_model_number) as model_number,
    b.match_confidence as matched_confidence_score,
    c.source_count,
    c.listing_title_count,
    c.source_count > 1 as is_cross_marketplace
from anchor_listing as a
inner join coverage as c using (product_key)
left join best_match as b using (product_key)
