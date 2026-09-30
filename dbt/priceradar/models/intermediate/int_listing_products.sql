-- Maps every distinct (marketplace, title) to the product it is an offer for.
--
-- Best Buy catalog items anchor the products. An eBay title with an accepted LLM match
-- joins the product of the Best Buy title it matched; when it matched several, the most
-- confident verdict wins. eBay titles without an accepted match stay products of their own.

with listings as (
    select * from {{ ref('stg_listings') }}
),

accepted_matches as (
    select
        ebay.product_name as ebay_title,
        bestbuy.product_name as bestbuy_title,
        m.canonical_name,
        m.model_number,
        m.confidence_score,
        m.matched_at
    from {{ ref('stg_matched') }} as m
    inner join listings as ebay on ebay.listing_id = m.ebay_listing_id
    inner join listings as bestbuy on bestbuy.listing_id = m.bestbuy_listing_id
    where m.is_accepted
),

best_match_per_ebay_title as (
    select *
    from accepted_matches
    qualify row_number() over (
        partition by ebay_title
        order by confidence_score desc, matched_at desc
    ) = 1
),

titles as (
    select distinct marketplace_source, product_name
    from listings
),

mapped as (
    select
        t.marketplace_source,
        t.product_name,
        case when m.bestbuy_title is not null then 'bestbuy' else t.marketplace_source end
            as anchor_source,
        coalesce(m.bestbuy_title, t.product_name) as anchor_title,
        m.canonical_name as match_canonical_name,
        m.model_number as match_model_number,
        m.confidence_score as match_confidence
    from titles as t
    left join best_match_per_ebay_title as m
        on t.marketplace_source = 'ebay'
        and t.product_name = m.ebay_title
)

select
    {{ dbt_utils.generate_surrogate_key(['anchor_source', 'anchor_title']) }} as product_key,
    *
from mapped
