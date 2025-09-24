# Imports
import duckdb
import logging

# Initializing logger
logging.basicConfig(
    level = logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename = 'clean.log'
)
logger = logging.getLogger(__name__)

# Function to clean tables
def clean():

    con = None

    # Try to connect to clean tables in duckdb
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database = 'emissions.duckdb', read_only = False)
        logger.info("Connected to DuckDB instance")


        for table in ["yellow_taxi_data", "green_taxi_data"]:

            # Remove any duplicate trips.
            con.execute(f"""
                -- Create temp table with distinct rows of the table
                CREATE TABLE temp AS
                SELECT DISTINCT * FROM {table};

                -- Drop the original table
                DROP TABLE {table};

                -- Rename temp to the original table's name
                ALTER TABLE temp RENAME to {table};      
            """)
            print(f"Deduped {table}")
            logger.info(f"Deduped {table}")



            # Remove any trips with 0 passengers.
            con.execute(f"""
                -- Create temp table with rows of the table with passengers > 0
                CREATE TABLE temp AS
                SELECT * FROM {table} WHERE passenger_count > 0;

                -- Drop the original table
                DROP TABLE {table};

                -- Rename temp to the original table's name
                ALTER TABLE temp RENAME to {table};    
            """)
            print(f"Removed trips with 0 passengers from {table}")
            logger.info(f"Removed trips with 0 passengers from {table}")



            # Remove any trips 0 miles in length.
            con.execute(f"""
                -- Create temp table with rows of the table with distance > 0
                CREATE TABLE temp AS
                SELECT * FROM {table} WHERE trip_distance > 0;

                -- Drop the original table
                DROP TABLE {table};

                -- Rename temp to the original table's name
                ALTER TABLE temp RENAME to {table};    
            """)
            print(f"Removed trips of 0 miles from {table}")
            logger.info(f"Removed trips of 0 miles from {table}")



            # Remove any trips longer than 100 miles in length.
            con.execute(f"""
                -- Create temp table with rows of the table with distance <= 100
                CREATE TABLE temp AS
                SELECT * FROM {table} WHERE trip_distance <= 100;

                -- Drop the original table
                DROP TABLE {table};

                -- Rename temp to the original table's name
                ALTER TABLE temp RENAME to {table};    
            """)
            print(f"Removed trips of > 100 miles from {table}")
            logger.info(f"Removed trips of > 100 miles from {table}")



            # Remove any trips lasting more than 1 day in length (86400 seconds).
            con.execute(f"""
                -- Create temp table with rows where the date difference
                -- between pickup and dropoff is <= 86400 seconds
                CREATE TABLE temp AS
                SELECT * FROM {table} WHERE date_diff('seconds', pickup_time, dropoff_time) <= 86400;

                -- Drop the original table
                DROP TABLE {table};

                -- Rename temp to the original table's name
                ALTER TABLE temp RENAME to {table};  
            """)
            print(f"Removed trips of > 1 day from {table}")
            logger.info(f"Removed trips of > 1 day from {table}")



            print(f"Confirmations for {table}:")
            logger.info(f"Confirmation for {table}:")

            # Confirm no duplicates
            num_dupes = con.execute(f"""
                SELECT COUNT(*)
                FROM {table};
            """).fetchone()[0] - con.execute(f"""
                SELECT COUNT(*)
                FROM (SELECT DISTINCT * FROM {table})                             
            """).fetchone()[0]
            print(f"\tNumber of duplicate rows: {num_dupes}")
            logger.info(f"\tNumber of duplicate rows: {num_dupes}")



            # Confirm no trips with 0 passengers
            no_pass_count = con.execute(f"""
                -- Getting the number of rows with 0 passengers
                SELECT COUNT(*) FROM {table}
                WHERE passenger_count == 0;
            """).fetchone()[0]
            print(f"\tNumber of trips with 0 passengers: {no_pass_count}")
            logger.info(f"\tNumber of trips with 0 passengers: {no_pass_count}")



            # Confirm no trips 0 miles in length
            num_0 = con.execute(f"""
                -- Getting the number of trips of 0 length
                SELECT COUNT(*) FROM {table}
                WHERE trip_distance == 0;                   
            """).fetchone()[0]
            print(f"\tNumber of trips of 0 miles in length: {num_0}")
            logger.info(f"\tNumber of trips of 0 miles in length: {num_0}")



            # Confirm no trips longer than 100 miles
            num_over_100 = con.execute(f"""
                -- Getting the number of trips longer than 100 miles
                SELECT COUNT(*) FROM {table}
                WHERE trip_distance > 100;   
            """).fetchone()[0]
            print(f"\tNumber of trips > 100 miles in length: {num_over_100}")
            logger.info(f"\tNumber of trips > 100 miles in length: {num_over_100}")
 


            # Confirm no trips lasting longer than a day
            num_over_day = con.execute(f"""
                -- Getting the number of trips lasting longer than a day
                SELECT COUNT(*) FROM {table}
                WHERE date_diff('seconds', pickup_time, dropoff_time) > 86400;
            """).fetchone()[0]
            print(f"\tNumber of trips lasting over a day: {num_over_day}")
            logger.info(f"\tNumber of trips lasting over a day: {num_over_day}")



    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    clean()

