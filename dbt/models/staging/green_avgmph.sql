{{ config(materialized='ephemeral') }}

SELECT 
    *,
    CASE
        WHEN EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) > 0
        THEN ROUND(trip_distance / (EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 3600.0), 3)
        ELSE NULL
    END AS avg_mph
FROM
    {{ ref('green_co2') }}
