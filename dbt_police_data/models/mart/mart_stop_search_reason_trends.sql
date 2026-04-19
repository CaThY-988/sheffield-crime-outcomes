with grouped as (

    select
        date_format(stop_search_datetime, 'yyyy-MM') as stop_search_month,
        coalesce(object_of_search, 'Unknown') as object_of_search,
        count(*) as stop_search_count
    from {{ ref('int_police_stop_and_search') }}
    group by 1, 2

)

select
    stop_search_month,
    object_of_search,
    stop_search_count,
    stop_search_count * 1.0
        / sum(stop_search_count) over (
            partition by stop_search_month
        ) as pct_of_month_total
from grouped