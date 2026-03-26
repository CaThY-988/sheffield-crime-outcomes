{{ config(materialized='view') }}

select
    crime_id,
    persistent_id,
    crime_category,
    crime_month,
    location_type,
    location_subtype,
    context,

    location.latitude as crime_latitude,
    location.longitude as crime_longitude,
    location.street.id as crime_street_id,
    location.street.name as crime_street_name,

    outcome_status.category as latest_outcome_category,
    outcome_status.date as latest_outcome_date

from {{ ref('stg_police_crimes') }}