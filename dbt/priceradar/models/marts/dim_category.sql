-- One row per product category.
select distinct
    {{ dbt_utils.generate_surrogate_key(['category']) }} as category_key,
    category as category_name
from {{ ref('stg_listings') }}
where category is not null
