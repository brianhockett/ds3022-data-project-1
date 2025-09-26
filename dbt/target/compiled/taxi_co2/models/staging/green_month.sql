

with __dbt__cte__green_co2 as (


SELECT
    green_taxi_data.*,
    (green_taxi_data.trip_distance * vehicle_emissions.co2_grams_per_mile) / 1000 AS trip_co2_kgs
FROM
    green_taxi_data
CROSS JOIN vehicle_emissions
WHERE vehicle_emissions.vehicle_type = 'green_taxi'
),  __dbt__cte__green_avgmph as (


SELECT 
    *,
    CASE
        WHEN EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) > 0
        THEN ROUND(trip_distance / (EXTRACT(EPOCH FROM (dropoff_time - pickup_time)) / 3600.0), 3)
        ELSE NULL
    END AS avg_mph
FROM
    __dbt__cte__green_co2
),  __dbt__cte__green_hour as (


SELECT 
    *,
    HOUR(pickup_time) AS hour_of_day
FROM
    __dbt__cte__green_avgmph
),  __dbt__cte__green_day as (


SELECT 
    *,
    DAYOFWEEK(pickup_time) AS day_of_week
FROM
    __dbt__cte__green_hour
),  __dbt__cte__green_weekyear as (


SELECT 
    *,
    WEEKOFYEAR(pickup_time) AS week_of_year
FROM
    __dbt__cte__green_day
) SELECT 
    *,
    MONTH(pickup_time) AS month_of_year
FROM
    __dbt__cte__green_weekyear