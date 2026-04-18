with base as (

    select
        crime_category,
        crime_month,
        latest_outcome_date,
        floor(
            months_between(
                to_date(latest_outcome_date),
                to_date(crime_month, 'yyyy-MM')
            )
        ) as months_to_outcome
    from {{ ref('int_police_crimes') }}
    where latest_outcome_date is not null
      and crime_month is not null

)

select
    crime_month,
    crime_category,
    count(*) as crime_count,
    avg(months_to_outcome) as avg_months_to_outcome,
    percentile_approx(months_to_outcome, 0.5) as median_months_to_outcome,
    min(months_to_outcome) as min_months_to_outcome,
    max(months_to_outcome) as max_months_to_outcome
from base
where months_to_outcome >= 0
group by 1, 2