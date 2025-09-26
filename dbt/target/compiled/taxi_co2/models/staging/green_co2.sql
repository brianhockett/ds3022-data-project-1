

SELECT
    green_taxi_data.*,
    (green_taxi_data.trip_distance * vehicle_emissions.co2_grams_per_mile) / 1000 AS trip_co2_kgs
FROM
    green_taxi_data
CROSS JOIN vehicle_emissions
WHERE vehicle_emissions.vehicle_type = 'green_taxi'