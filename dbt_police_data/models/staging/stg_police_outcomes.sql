{{ config(materialized='view') }}

select
    category,
    cast(date as string) as outcome_date,
    cast(person_id as string) as person_id,
    crime
from {{ source('src_police', 'outcome_data') }}