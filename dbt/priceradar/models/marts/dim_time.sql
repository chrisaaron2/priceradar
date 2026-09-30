-- One row per distinct scrape timestamp. day_of_week follows BigQuery: 1 = Sunday, 7 = Saturday.
with timestamps as (
    select distinct scraped_at
    from {{ ref('stg_listings') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['scraped_at']) }} as time_key,
    scraped_at,
    date(scraped_at) as date_day,
    extract(hour from scraped_at) as hour,
    extract(dayofweek from scraped_at) as day_of_week,
    format_timestamp('%A', scraped_at) as day_name,
    extract(dayofweek from scraped_at) in (1, 7) as is_weekend,
    extract(isoweek from scraped_at) as week_number,
    extract(month from scraped_at) as month
from timestamps
