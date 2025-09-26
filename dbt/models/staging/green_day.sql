{{ config(materialized='ephemeral') }}

SELECT 
    *,
    DAYOFWEEK(pickup_time) AS day_of_week
FROM
    {{ref('green_hour')}}