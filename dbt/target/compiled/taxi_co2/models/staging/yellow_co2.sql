

SELECT
    yellow_taxi_data.*,
    (yellow_taxi_data.trip_distance * vehicle_emissions.co2_grams_per_mile) / 1000 AS trip_co2_kgs
FROM
    yellow_taxi_data
CROSS JOIN vehicle_emissions
WHERE vehicle_emissions.vehicle_type = 'yellow_taxi'