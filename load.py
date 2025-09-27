# Imports
import duckdb        # For interacting with DuckDB databases
import os            # For filesystem operations
import logging       # For logging messages and errors
import time          # For adding delays between requests to avoid 403 errors

# Initializing logger
logging.basicConfig(
    level = logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',  # Timestamp, level, and message format
    filename = 'load.log'                                # Log output file
)
logger = logging.getLogger(__name__)  # Create a logger instance

# Function to load taxi and emissions data into DuckDB tables and display summary statistics
def load():

    # Define the years and months to process
    years = ['2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022', '2023', '2024']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    # Initialize DuckDB connection placeholder
    con = None

    # Try to load data into DuckDB
    try:
        # Connect to local DuckDB instance (read/write mode)
        con = duckdb.connect(database = 'emissions.duckdb', read_only = False)
        print("Connected to DuckDB instance")
        logger.info("Connected to DuckDB instance")

        # Drop tables if they exist from a previous run
        con.execute(f"""
            -- Drop the yellow taxi table if it exists
            DROP TABLE IF EXISTS yellow_taxi_data;              

            -- Drop the green taxi table if it exists
            DROP TABLE IF EXISTS green_taxi_data;   

            -- Drop the vehicle_emissions table if it exists
            DROP TABLE IF EXISTS vehicle_emissions;
        """)
        print("Dropped tables if they existed already")
        logger.info("Dropped tables if they existed already")
        
        # Create empty taxi tables with correct schema
        con.execute("""
            -- Creating empty yellow taxi table
            CREATE TABLE IF NOT EXISTS yellow_taxi_data (
                pickup_time TIMESTAMP,
                dropoff_time TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DOUBLE);

            -- Creating empty green taxi table
            CREATE TABLE IF NOT EXISTS green_taxi_data (
                pickup_time TIMESTAMP,
                dropoff_time TIMESTAMP,
                passenger_count INTEGER,
                trip_distance DOUBLE);
        """)
        print("Created empty tables: yellow_taxi_data and green_taxi_data")
        logger.info("Created empty tables: yellow_taxi_data and green_taxi_data")

        # Loop through years and months to load taxi data
        for year in years:
            for month in months:

                # Construct file paths dynamically based on year and month
                file_path_yellow = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet'
                file_path_green = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet'

                # Append yellow taxi data to table
                con.execute(f"""
                    -- Insert data into yellow taxi table from parquet
                    INSERT INTO yellow_taxi_data
                    SELECT
                        tpep_pickup_datetime AS pickup_time,
                        tpep_dropoff_datetime AS dropoff_time,
                        passenger_count,
                        trip_distance   
                    FROM parquet_scan(?);
                """, [file_path_yellow])

                # Append green taxi data to table
                con.execute(f"""
                    -- Insert data into green taxi table from parquet
                    INSERT INTO green_taxi_data
                    SELECT
                        lpep_pickup_datetime AS pickup_time,
                        lpep_dropoff_datetime AS dropoff_time,
                        passenger_count,
                        trip_distance
                    FROM parquet_scan(?);
                """, [file_path_green])

                print(f"Appended yellow and green taxi data for {month}/{year} to respective tables")
                logger.info(f"Appended yellow and green taxi data for {month}/{year} to respective tables")
                
                # Sleep for 30 seconds to avoid 403 errors from server
                time.sleep(30)
        
        print("Taxi data loading completed successfully")
        logger.info("Taxi data loading completed successfully")

        # Create vehicle_emissions table from CSV file
        con.execute(f"""
            -- Create vehicle_emissions table
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv('data/vehicle_emissions.csv');       
        """)
        print("Created vehicle_emissions table from csv file")
        logger.info("Created vehicle_emissions table from csv file")

        print("load.py successfully completed data loading")
        logger.info("load.py successfully completed data loading")

        # Summary Statistics

        # Print/log number of rows in each table
        for table in ["yellow_taxi_data", "green_taxi_data", "vehicle_emissions"]:
            num_rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{num_rows} rows in {table}")
            logger.info(f"{num_rows} rows in {table}")

        # Print/log averages for columns in taxi tables
        for table in ["yellow_taxi_data", "green_taxi_data"]:
            avg = con.execute(f"""
                -- Get averages for important columns
                SELECT
                    AVG(pickup_time) AS average_pickup_time,
                    AVG(dropoff_time) AS average_dropoff_time,
                    AVG(passenger_count) AS average_passenger_count,
                    AVG(trip_distance) AS average_trip_distance
                FROM {table};
                """).fetchone()
            
            # Printing averages
            print(f"Averages for {table}:")
            print(f"\tPickup Time: {avg[0]}")
            print(f"\tDropoff Time: {avg[1]}")
            print(f"\tPassenger Count: {round(avg[2], 3)}")
            print(f"\tTrip Distance: {round(avg[3], 3)}")

            # Logging averages
            logger.info(f"Averages for {table}:")
            logger.info(f"\tPickup Time: {avg[0]}")
            logger.info(f"\tDropoff Time: {avg[1]}")
            logger.info(f"\tPassenger Count: {round(avg[2], 3)}")
            logger.info(f"\tTrip Distance: {round(avg[3], 3)}")

        # Print/log averages for vehicle_emissions table
        avg_emissions = con.execute(f"""
            -- Get averages for vehicle_emissions columns
            SELECT
                AVG(mpg_city) AS average_mpg_city,
                AVG(mpg_highway) AS average_mpg_highway,
                AVG(co2_grams_per_mile) AS average_co2_grams_per_mile,
                AVG(vehicle_year_avg) AS average_vehicle_year_avg
            FROM vehicle_emissions;
            """).fetchone()
        
        # Printing averages
        print(f"Averages for vehicle_emissions:")
        print(f"\tMPG City: {round(avg_emissions[0], 3)}")
        print(f"\tMPG Highway: {round(avg_emissions[1], 3)}")
        print(f"\tCO2 grams per mile: {round(avg_emissions[2], 3)}")
        print(f"\tVehicle Year Average: {round(avg_emissions[3], 1)}")

        # Logging averages
        logger.info(f"Averages for vehicle_emissions:")
        logger.info(f"\tMPG City: {round(avg_emissions[0], 3)}")
        logger.info(f"\tMPG Highway: {round(avg_emissions[1], 3)}")
        logger.info(f"\tCO2 grams per mile: {round(avg_emissions[2], 3)}")
        logger.info(f"\tVehicle Year Average: {round(avg_emissions[3], 1)}")

        logger.info("load.py script is complete")

    # Error handling
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

# Run load function if script is executed directly
if __name__ == "__main__":
    load()
