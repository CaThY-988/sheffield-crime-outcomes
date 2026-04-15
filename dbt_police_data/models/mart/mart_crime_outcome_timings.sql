with base as (

    select
        crime_category,
        crime_month,
        datediff(
            to_date(latest_outcome_date),
            to_date(crime_month, 'yyyy-MM')
        ) as days_to_outcome
    from {{ ref('int_police_crimes') }}
    where latest_outcome_date is not null

)

select
    crime_month,
    crime_category,
    count(*) as crime_count,
    avg(days_to_outcome) as avg_days_to_outcome,
    percentile_approx(days_to_outcome, 0.5) as median_days_to_outcome,
    min(days_to_outcome) as min_days_to_outcome,
    max(days_to_outcome) as max_days_to_outcome
from base
where days_to_outcome >= 0
group by 1, 2