with grouped as (

    select
        coalesce(object_of_search, 'Unknown') as object_of_search,
        coalesce(outcome, 'Unknown') as outcome,
        count(*) as stop_search_count
    from {{ ref('int_police_stop_and_search') }}
    group by 1, 2

)

select
    object_of_search,
    outcome,
    stop_search_count,
    stop_search_count * 1.0
        / sum(stop_search_count) over (
            partition by object_of_search
        ) as pct_within_object_of_search
from grouped