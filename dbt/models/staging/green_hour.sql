{{ config(materialized='ephemeral') }}

SELECT 
    *,
    HOUR(pickup_time) AS hour_of_day
FROM
    {{ref('green_avgmph')}}