{{ config(materialized='view') }}

with parsed as (

    select
        *,
        from_json(
            category,
            'code STRING, name STRING'
        ) as category_struct,
        from_json(
            crime,
            '''
            id BIGINT,
            persistent_id STRING,
            category STRING,
            location_type STRING,
            location_subtype STRING,
            context STRING,
            month STRING,
            location STRUCT<
                latitude: STRING,
                longitude: STRING,
                street: STRUCT<
                    id: BIGINT,
                    name: STRING
                >
            >
            '''
        ) as crime_struct

    from {{ ref('stg_police_outcomes') }}

)

select
    category_struct.code as outcome_category_code,
    category_struct.name as outcome_category_name,
    outcome_date,
    person_id,

    crime_struct.id as crime_id,
    crime_struct.persistent_id as crime_persistent_id,
    crime_struct.category as crime_category,
    crime_struct.location_type as crime_location_type,
    crime_struct.location_subtype as crime_location_subtype,
    crime_struct.context as crime_context,
    crime_struct.month as crime_month,

    crime_struct.location.latitude as crime_latitude,
    crime_struct.location.longitude as crime_longitude,
    crime_struct.location.street.id as crime_street_id,
    crime_struct.location.street.name as crime_street_name

from parsed