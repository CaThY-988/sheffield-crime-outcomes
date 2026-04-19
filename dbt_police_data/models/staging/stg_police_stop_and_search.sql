{{ config(materialized='view') }}

select
    cast(age_range as string) as age_range,
    cast(officer_defined_ethnicity as string) as officer_defined_ethnicity,
    cast(involved_person as boolean) as involved_person,
    cast(self_defined_ethnicity as string) as self_defined_ethnicity,
    cast(gender as string) as gender,
    cast(legislation as string) as legislation,
    cast(outcome_linked_to_object_of_search as boolean) as outcome_linked_to_object_of_search,
    cast(datetime as timestamp) as stop_search_datetime,
    outcome_object,
    location,
    cast(object_of_search as string) as object_of_search,
    cast(operation as boolean) as operation,
    cast(outcome as string) as outcome,
    cast(type as string) as stop_search_type,
    cast(operation_name as string) as operation_name,
    cast(removal_of_more_than_outer_clothing as boolean) as removal_of_more_than_outer_clothing
from {{ source('src_police', 'stop_and_search_data') }}