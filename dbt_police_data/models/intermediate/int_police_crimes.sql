with parsed as (

    select
        *,
        from_json(
            location,
            'latitude STRING, longitude STRING, street STRUCT<id: BIGINT, name: STRING>'
        ) as location_struct,
        from_json(
            outcome_status,
            'category STRING, date STRING'
        ) as outcome_struct

    from {{ ref('stg_police_crimes') }}

)

select
    crime_id,
    persistent_id,
    crime_category,
    crime_month,
    location_type,
    location_subtype,
    context,
    
    location_struct.latitude as crime_latitude,
    location_struct.longitude as crime_longitude,
    location_struct.street.id as crime_street_id,
    location_struct.street.name as crime_street_name,

    outcome_struct.category as latest_outcome_category,
    outcome_struct.date as latest_outcome_date

from parsed