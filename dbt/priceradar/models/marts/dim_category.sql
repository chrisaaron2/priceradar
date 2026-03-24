with categories as (
    select distinct
        category
    from {{ ref('stg_listings') }}
    where category is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['category']) }} as category_key,
        category as category_name,
        cast(null as string) as subcategory
    from categories
)

select * from final