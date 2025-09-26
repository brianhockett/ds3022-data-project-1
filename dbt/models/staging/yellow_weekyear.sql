{{ config(materialized='ephemeral') }}

SELECT 
    *,
    WEEKOFYEAR(pickup_time) AS week_of_year
FROM
    {{ref('yellow_day')}}