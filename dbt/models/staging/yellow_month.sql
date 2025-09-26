{{ config(materialized='ephemeral') }}

SELECT 
    *,
    MONTH(pickup_time) AS month_of_year
FROM
    {{ref('yellow_weekyear')}}