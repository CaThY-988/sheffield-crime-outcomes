select
    crime_id,
    crime_month,
    crime_category,
    crime_latitude,
    crime_longitude,
    crime_street_name,
    latest_outcome_category
from {{ ref('int_police_crimes') }}
