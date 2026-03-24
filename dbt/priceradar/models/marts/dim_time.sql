with timestamps as (
    select distinct
        scraped_at
    from {{ ref('stg_listings') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['scraped_at']) }} as time_key,
        scraped_at,
        extract(hour from scraped_at) as hour,
        extract(dayofweek from scraped_at) as day_of_week,
        case when extract(dayofweek from scraped_at) in (1, 7) then true else false end as is_weekend,
        extract(week from scraped_at) as week_number,
        extract(month from scraped_at) as month
    from timestamps
)

select * from final