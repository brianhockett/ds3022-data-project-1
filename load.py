# Imports
import duckdb
import os
import logging
import time

# Initializing logger
logging.basicConfig(
    level = logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename = 'load.log'
)
logger = logging.getLogger(__name__)

# Function to load taxi and emissions data into duckdb database tables, and display summary statistics/info to console and log
def load():

    # Define the years and months to process
    #years = ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']
    #months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    years = ['2024']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    # Initialize DuckDB connection
    con = None

    # Try to load data into DuckDB
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database = 'emissions.duckdb', read_only = False)
        logger.info("Connected to DuckDB instance")

        # Drop the tables if they already exist from a previous run
        con.execute(f"""
            -- Drop the yellow table if it exists
            DROP TABLE IF EXISTS yellow_taxi_data;              

            -- Drop the green table if it exists
            DROP TABLE IF EXISTS green_taxi_data;   

            -- Drop the vehicle_emissions table if it exists
            DROP TABLE IF EXISTS vehicle_emissions;
        """)
        logger.info("Dropped tables if they existed already")

        # Loop through years and months to load the taxi data
        for year in years:
            for month in months:

                # Construct file paths with variable year and month
                file_path_yellow = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet'
                file_path_green = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet'

                # If first iteration, new tables are created for the yellow and green taxis
                if year == years[0] and month == months[0]:
                    con.execute(f"""
                        -- Create the yellow table using specific columns
                        CREATE TABLE yellow_taxi_data AS 
                        SELECT
                            tpep_pickup_datetime AS pickup_time,
                            tpep_dropoff_datetime AS dropoff_time,
                            passenger_count,
                            trip_distance
                        FROM read_parquet('{file_path_yellow}');

                        -- Create the green table using specific columns
                        CREATE TABLE green_taxi_data AS 
                        SELECT
                            lpep_pickup_datetime AS pickup_time,
                            lpep_dropoff_datetime AS dropoff_time,
                            passenger_count,
                            trip_distance
                        FROM read_parquet('{file_path_green}');
                    """)
                    logger.info(f"Created yellow_taxi_data and green_taxi_data tables with data from {month}/{year}")
                    time.sleep(30)

                # Else, appends data to the tables made in the first iteration
                else:
                    con.execute(f"""
                        -- Append data to yellow table
                        INSERT INTO yellow_taxi_data
                        SELECT
                            tpep_pickup_datetime AS pickup_time,
                            tpep_dropoff_datetime AS dropoff_time,
                            passenger_count,
                            trip_distance   
                        FROM read_parquet('{file_path_yellow}');

                        -- Append data to green table
                        INSERT INTO green_taxi_data
                        SELECT
                            lpep_pickup_datetime AS pickup_time,
                            lpep_dropoff_datetime AS dropoff_time,
                            passenger_count,
                            trip_distance
                        FROM read_parquet('{file_path_green}');
                    """)
                    logger.info(f"Appended yellow and green taxi data for {month}/{year} to respective tables")
                    time.sleep(30)
        
        logger.info("Taxi data loading completed successfully")

        # Creating vehicle_emissions table using the .csv file
        con.execute(f"""
            -- Create vehicle emissions table
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv('data/vehicle_emissions.csv');       
        """)
        logger.info("Created vehicle_emissions table from csv file")


        logger.info("load.py successfully completed data loading")


        # Summary Statistics

        # Printing/logging the number of rows in each table (yellow, green, emissions)
        for table in ["yellow_taxi_data", "green_taxi_data", "vehicle_emissions"]:
            num_rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{num_rows} rows in {table}")
            logger.info(f"{num_rows} rows in {table}")

        # Printing/logging averages of important columns in taxi data
        for table in ["yellow_taxi_data", "green_taxi_data"]:
            avg = con.execute(f"""
                -- Get averages for columns in taxi data tables
                SELECT
                    AVG(pickup_time) AS average_pickup_time,
                    AVG(dropoff_time) AS average_dropoff_time,
                    AVG(passenger_count) AS average_passenger_count,
                    AVG(trip_distance) AS average_trip_distance
                FROM {table};
                """).fetchone()
            
            # Printing
            print(f"Averages for {table}:")
            print(f"\tPickup Time: {avg[0]}")
            print(f"\tDropoff Time: {avg[1]}")
            print(f"\tPassenger Count: {round(avg[2], 3)}")
            print(f"\tTrip Distance: {round(avg[3], 3)}")

            # Logging
            logger.info(f"Averages for {table}:")
            logger.info(f"\tPickup Time: {avg[0]}")
            logger.info(f"\tDropoff Time: {avg[1]}")
            logger.info(f"\tPassenger Count: {round(avg[2], 3)}")
            logger.info(f"\tTrip Distance: {round(avg[3], 3)}")

        # Printing/logging averages for vehicle_emmisions table
        avg_emissions = con.execute(f"""
            -- Get averages for columns in vehicle_emissions table
            SELECT
                AVG(mpg_city) AS average_mpg_city,
                AVG(mpg_highway) AS average_mpg_highway,
                AVG(co2_grams_per_mile) AS average_co2_grams_per_mile,
                AVG(vehicle_year_avg) AS average_vehicle_year_avg
            FROM vehicle_emissions;
            """).fetchone()
        
        # Printing
        print(f"Averages for vehicle_emissions:")
        print(f"\tMPG City: {round(avg_emissions[0], 3)}")
        print(f"\tMPG Highway: {round(avg_emissions[1], 3)}")
        print(f"\tCO2 grams per mile: {round(avg_emissions[2], 3)}")
        print(f"\tVehicle Year Average: {round(avg_emissions[3], 1)}")

        # Logging
        logger.info(f"Averages for vehicle_emissions:")
        logger.info(f"\tMPG City: {round(avg_emissions[0], 3)}")
        logger.info(f"\tMPG Highway: {round(avg_emissions[1], 3)}")
        logger.info(f"\tCO2 grams per mile: {round(avg_emissions[2], 3)}")
        logger.info(f"\tVehicle Year Average: {round(avg_emissions[3], 1)}")


        logger.info("load.py script is complete")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    load()