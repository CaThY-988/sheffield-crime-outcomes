{{ config(materialized='view') }}

select
    cast(category as string) as crime_category,
    cast(persistent_id as string) as persistent_id,
    cast(location_subtype as string) as location_subtype,
    cast(id as bigint) as crime_id,
    location,
    cast(context as string) as context,
    cast(month as string) as crime_month,
    cast(location_type as string) as location_type,
    outcome_status
from {{ source('src_police', 'crime_data') }}