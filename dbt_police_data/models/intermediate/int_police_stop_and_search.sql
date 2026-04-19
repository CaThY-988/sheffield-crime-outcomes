with parsed as (

    select
        *,
        from_json(
            location,
            'latitude STRING, longitude STRING, street STRUCT<id: BIGINT, name: STRING>'
        ) as location_struct,
        from_json(
            outcome_object,
            'id STRING, name STRING'
        ) as outcome_object_struct

    from {{ ref('stg_police_stop_and_search') }}

)

select
    age_range,
    officer_defined_ethnicity,
    involved_person,
    self_defined_ethnicity,
    gender,
    legislation,
    outcome_linked_to_object_of_search,
    stop_search_datetime,
    object_of_search,
    operation,
    outcome,
    stop_search_type,
    operation_name,
    removal_of_more_than_outer_clothing,

    location_struct.latitude as stop_search_latitude,
    location_struct.longitude as stop_search_longitude,
    location_struct.street.id as stop_search_street_id,
    location_struct.street.name as stop_search_street_name,

    outcome_object_struct.id as outcome_object_id,
    outcome_object_struct.name as outcome_object_name

from parsed