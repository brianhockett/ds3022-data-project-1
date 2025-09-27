# Imports
import duckdb        # For interacting with DuckDB databases
import logging       # For logging messages and errors
import os            # For filesystem operations like creating directories

# Initializing logger
logging.basicConfig(
    level = logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',  # Timestamp, level, and message format
    filename = 'clean.log'                               # Log output file
)
logger = logging.getLogger(__name__)  # Create a logger instance

# Function to clean taxi data tables prior to transformation and analysis
def clean():

    con = None  # Placeholder for DuckDB connection

    # Try to connect to DuckDB and clean tables
    try:
        # Connect to local DuckDB instance (read/write mode)
        con = duckdb.connect(database = 'emissions.duckdb', read_only = False)

        # Ensure temporary directory exists for DuckDB operations
        os.makedirs("tmp", exist_ok = True)
        con.execute("PRAGMA temp_directory='tmp';")

        print("Connected to DuckDB instance")
        logger.info("Connected to DuckDB instance")

        # Loop through taxi data tables (yellow and green) individually
        for table in ["yellow_taxi_data", "green_taxi_data"]:

            # Perform all cleaning in a single query
            con.execute(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT DISTINCT *
                FROM {table}
                WHERE passenger_count > 0                       -- Remove trips with 0 passengers
                  AND trip_distance > 0                         -- Remove trips of 0 miles
                  AND trip_distance <= 100                      -- Remove trips longer than 100 miles
                  AND date_diff('seconds', pickup_time, dropoff_time) <= 86400;  -- Remove trips longer than 1 day
            """)
            print(f"Cleaned {table} in one pass")
            logger.info(f"Cleaned {table} in one pass")

            print(f"Confirmations for {table}:")
            logger.info(f"Confirmation for {table}:")

            # Confirm no duplicates remain
            num_dupes = con.execute(f"""
                -- Get total number of rows
                SELECT COUNT(*)
                FROM {table};
            """).fetchone()[0] - con.execute(f"""
                -- Get number of distinct rows; subtraction gives duplicates
                SELECT COUNT(*)
                FROM (SELECT DISTINCT * FROM {table})                             
            """).fetchone()[0]
            print(f"\tNumber of duplicate rows: {num_dupes}")
            logger.info(f"\tNumber of duplicate rows: {num_dupes}")

            # Confirm no trips with 0 passengers
            no_pass_count = con.execute(f"""
                -- Count rows with 0 passengers
                SELECT COUNT(*) FROM {table}
                WHERE passenger_count == 0;
            """).fetchone()[0]
            print(f"\tNumber of trips with 0 passengers: {no_pass_count}")
            logger.info(f"\tNumber of trips with 0 passengers: {no_pass_count}")

            # Confirm no trips of 0 miles in length
            num_0 = con.execute(f"""
                -- Count rows with trip distance of 0
                SELECT COUNT(*) FROM {table}
                WHERE trip_distance == 0;                   
            """).fetchone()[0]
            print(f"\tNumber of trips of 0 miles in length: {num_0}")
            logger.info(f"\tNumber of trips of 0 miles in length: {num_0}")

            # Confirm no trips longer than 100 miles
            num_over_100 = con.execute(f"""
                -- Count rows with trip distance > 100
                SELECT COUNT(*) FROM {table}
                WHERE trip_distance > 100;   
            """).fetchone()[0]
            print(f"\tNumber of trips > 100 miles in length: {num_over_100}")
            logger.info(f"\tNumber of trips > 100 miles in length: {num_over_100}")

            # Confirm no trips lasting longer than a day
            num_over_day = con.execute(f"""
                -- Count rows with trip duration > 86400 seconds (1 day)
                SELECT COUNT(*) FROM {table}
                WHERE date_diff('seconds', pickup_time, dropoff_time) > 86400;
            """).fetchone()[0]
            print(f"\tNumber of trips lasting over a day: {num_over_day}")
            logger.info(f"\tNumber of trips lasting over a day: {num_over_day}")

        print(f"clean.py script is complete")
        logger.info(f"clean.py script is complete")

    # Handle any errors during cleaning
    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


# Run the cleaning function if script is executed directly
if __name__ == "__main__":
    clean()
