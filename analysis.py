# Imports
import duckdb          # For interacting with DuckDB databases
import logging         # For logging messages and errors
import pandas as pd    # For data manipulation and analysis
import matplotlib.pyplot as plt  # For plotting data

# Initializing logger
logging.basicConfig(
    level = logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',  # Timestamp, level, and message format
    filename = 'analysis.log'                            # Log output file
)
logger = logging.getLogger(__name__)  # Create a logger instance

# Function to analyze taxi carbon emissions data
def analyze():

    con = None  # Placeholder for DuckDB connection

    # Try to connect to clean tables in DuckDB
    try:
        # Connect to local DuckDB instance (read/write mode)
        con = duckdb.connect(database = 'emissions.duckdb', read_only = False)
        print("Connected to DuckDB instance")
        logger.info("Connected to DuckDB instance")

        # Loop over taxi data tables (yellow and green)
        for table, color in zip(['final_yellow_data', 'final_green_data'], ['YELLOW', 'GREEN']):
            
            print(f"Analysis for {color} taxi data:")
            logger.info(f"Analysis for {color} taxi data:")

            # 1. Single largest carbon producing trip
            maxco2_trip = con.execute(f"""
                -- Get the trip with the maximum CO2 produced
                SELECT pickup_time, dropoff_time, trip_co2_kgs
                FROM {table}
                ORDER BY trip_co2_kgs DESC
                LIMIT 1;
            """).fetchone()
            pickup_time, dropoff_time, co2_kgs = maxco2_trip
            print(f"\tThe {color} taxi trip with the single largest carbon produced was the trip with pickup time: {pickup_time}, drop off time: {dropoff_time}, and  {round(co2_kgs, 3)} kgs of carbon produced")
            logger.info(f"\tThe {color} taxi trip with the single largest carbon produced was the trip with pickup time: {pickup_time}, drop off time: {dropoff_time}, and  {round(co2_kgs, 3)} kgs of carbon produced")

            # 2. Carbon heaviest and lightest hour of the day
            co2_heavy_hour = con.execute(f"""
                -- Get hour with highest average CO2
                SELECT hour_of_day
                FROM {table}
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgs) DESC
                LIMIT 1;
            """).fetchone()[0]

            co2_light_hour = con.execute(f"""
                -- Get hour with lowest average CO2
                SELECT hour_of_day
                FROM {table}
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgs) ASC
                LIMIT 1;
            """).fetchone()[0]
            for cond, val in zip(['HEAVIEST', 'LIGHTEST'], [co2_heavy_hour, co2_light_hour]):
                print(f"\tThe {cond} carbon hour of the day for {color} trips was: {val}")
                logger.info(f"\tThe {cond} carbon hour of the day for {color} trips was: {val}")

            # 3. Carbon heaviest and lightest day of the week
            co2_heavy_day = con.execute(f"""
                -- Get day of week with highest average CO2
                SELECT day_of_week
                FROM {table}
                GROUP BY day_of_week
                ORDER BY AVG(trip_co2_kgs) DESC
                LIMIT 1;
            """).fetchone()[0]

            co2_light_day = con.execute(f"""
                -- Get day of week with lowest average CO2
                SELECT day_of_week
                FROM {table}
                GROUP BY day_of_week
                ORDER BY AVG(trip_co2_kgs) ASC
                LIMIT 1;
            """).fetchone()[0]
            for cond, val in zip(['HEAVIEST', 'LIGHTEST'], [co2_heavy_day, co2_light_day]):
                print(f"\tThe {cond} carbon day of the week for {color} trips was: {val}")
                logger.info(f"\tThe {cond} carbon day of the week for {color} trips was: {val}")

            # 4. Carbon heaviest and lightest week of the year
            co2_heavy_week = con.execute(f"""
                -- Get week of year with highest average CO2
                SELECT week_of_year
                FROM {table}
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgs) DESC
                LIMIT 1;
            """).fetchone()[0]

            co2_light_week = con.execute(f"""
                -- Get week of year with lowest average CO2
                SELECT week_of_year
                FROM {table}
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgs) ASC
                LIMIT 1;
            """).fetchone()[0]
            for cond, val in zip(['HEAVIEST', 'LIGHTEST'], [co2_heavy_week, co2_light_week]):
                print(f"\tThe {cond} carbon week of the year for {color} trips was: {val}")
                logger.info(f"\tThe {cond} carbon week of the year for {color} trips was: {val}")

            # 5. Carbon heaviest and lightest month of the year
            co2_heavy_month = con.execute(f"""
                -- Get month of year with highest average CO2
                SELECT month_of_year
                FROM {table}
                GROUP BY month_of_year
                ORDER BY AVG(trip_co2_kgs) DESC
                LIMIT 1;
            """).fetchone()[0]

            co2_light_month = con.execute(f"""
                -- Get month of year with lowest average CO2
                SELECT month_of_year
                FROM {table}
                GROUP BY month_of_year
                ORDER BY AVG(trip_co2_kgs) ASC
                LIMIT 1;
            """).fetchone()[0]
            for cond, val in zip(['HEAVIEST', 'LIGHTEST'], [co2_heavy_month, co2_light_month]):
                print(f"\tThe {cond} carbon month of the year for {color} trips was: {val}")
                logger.info(f"\tThe {cond} carbon month of the year for {color} trips was: {val}")    

        # 6. Time-series plot of total CO2 by year for yellow and green taxis

        # Aggregate yellow taxi data per year
        yellow_agg = con.execute("""
                -- Sum CO2 by year for yellow taxis (2015-2024)
                SELECT
                    EXTRACT(YEAR FROM pickup_time) AS year,
                    'yellow' AS color,
                    SUM(trip_co2_kgs) AS trip_co2_kgs
                FROM final_yellow_data
                WHERE EXTRACT(YEAR FROM pickup_time) >= 2015 AND EXTRACT(YEAR FROM pickup_time) <= 2024
                GROUP BY year;
            """).df()

        # Aggregate green taxi data per year
        green_agg = con.execute("""
                -- Sum CO2 by year for green taxis (2015-2024)
                SELECT
                    EXTRACT(YEAR FROM pickup_time) AS year,
                    'green' AS color,
                    SUM(trip_co2_kgs) AS trip_co2_kgs
                FROM final_green_data
                WHERE EXTRACT(YEAR FROM pickup_time) >= 2015 AND EXTRACT(YEAR FROM pickup_time) <= 2024
                GROUP BY year;
            """).df()

        # Combine yellow and green data
        yearly_sum = pd.concat([yellow_agg, green_agg], axis=0, ignore_index=True)

        # Plot CO2 time-series by taxi color
        plt.figure(figsize = (12, 8))
        for color in yearly_sum['color'].unique():
            subset = yearly_sum[yearly_sum['color'] == color]
            plt.plot(subset['year'], subset['trip_co2_kgs'], marker='o', label=color, color=color)
        plt.xlabel('Year')
        plt.ylabel('Sum of CO2 Production (Kg)')
        plt.title("Total CO2 Production by Year")
        plt.legend()
        plt.savefig("./co2_timeseries_analysis.png")

        print("Plotted time-series analysis and saved as co2_timeseries_analysis.png")
        logger.info("Plotted time-series analysis and saved as co2_timeseries_analysis.png")

    except Exception as e:
        # Handle any errors during analysis
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


# Run the analysis function if script is executed directly
if __name__ == "__main__":
    analyze()
