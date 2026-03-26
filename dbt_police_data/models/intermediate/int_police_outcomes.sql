{{ config(materialized='view') }}

select
    category.code as outcome_category_code,
    category.name as outcome_category_name,
    outcome_date,
    person_id,

    crime.id as crime_id,
    crime.persistent_id as crime_persistent_id,
    crime.category as crime_category,
    crime.location_type as crime_location_type,
    crime.location_subtype as crime_location_subtype,
    crime.context as crime_context,
    crime.month as crime_month,

    crime.location.latitude as crime_latitude,
    crime.location.longitude as crime_longitude,
    crime.location.street.id as crime_street_id,
    crime.location.street.name as crime_street_name

from {{ ref('stg_police_outcomes') }}