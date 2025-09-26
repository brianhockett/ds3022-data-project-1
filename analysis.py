# Imports
import duckdb
import logging
import pandas as pd
import matplotlib.pyplot as plt

# Initializing logger
logging.basicConfig(
    level = logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename = 'analysis.log'
)
logger = logging.getLogger(__name__)

# Function to analyze data
def analyze():

    con = None

    # Try to connect to clean tables in duckdb
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database = 'emissions.duckdb', read_only = False)
        print("Connected to DuckDB instance")
        logger.info("Connected to DuckDB instance")


        for table, color in zip(['final_yellow_data', 'final_green_data'], ['YELLOW', 'GREEN']):
            
            print(f"Analysis for {color} taxi data")
            logger.info(f"Analysis for {color} taxi data:")

            # 1. Single largest carbon producing trip
            maxco2_trip = con.execute(f"""
                -- Getting the trip with the max trip_co2_kgs
                SELECT MAX(trip_co2_kgs)
                FROM {table}; 
            """).fetchone()[0]
            print(f"\tThe single largest carbon producing trip for {color} trips was: {round(maxco2_trip, 3)}")
            logger.info(f"\tThe single largest carbon producing trip for {color} trips was: {round(maxco2_trip, 3)}")



            # 2. Carbon heaviest hour of the day
            co2_heavy_hour = con.execute(f"""
                -- Getting the heaviest hour of the day
                SELECT hour_of_day
                FROM {table}
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgs) DESC
                LIMIT 1;
            """).fetchone()[0]

            # 2. Carbon lightest hour of the day
            co2_light_hour = con.execute(f"""
                -- Getting the lightest hour of the day
                SELECT hour_of_day
                FROM {table}
                GROUP BY hour_of_day
                ORDER BY AVG(trip_co2_kgs) ASC
                LIMIT 1;
            """).fetchone()[0]
            for cond, val in zip(['HEAVIEST', 'LIGHTEST'], [co2_heavy_hour, co2_light_hour]):
                print(f"\tThe {cond} carbon hour of the day for {color} trips was: {val}")
                logger.info(f"\tThe {cond} carbon hour of the day for {color} trips was: {val}")



            # 3. Carbon heaviest day of the week
            co2_heavy_day = con.execute(f"""
                -- Getting the heaviest day of the week
                SELECT day_of_week
                FROM {table}
                GROUP BY day_of_week
                ORDER BY AVG(trip_co2_kgs) DESC
                LIMIT 1;
            """).fetchone()[0]
            
            # 3. Carbon lightest day of the week
            co2_light_day = con.execute(f"""
                -- Getting the lightest day of the week
                SELECT day_of_week
                FROM {table}
                GROUP BY day_of_week
                ORDER BY AVG(trip_co2_kgs) ASC
                LIMIT 1;
            """).fetchone()[0]
            for cond, val in zip(['HEAVIEST', 'LIGHTEST'], [co2_heavy_day, co2_light_day]):
                print(f"\tThe {cond} carbon day of the week for {color} trips was: {val}")
                logger.info(f"\tThe {cond} carbon day of the week for {color} trips was: {val}")



            # 4. Carbon heaviest week of the year
            co2_heavy_week = con.execute(f"""
                -- Getting the heaviest week of the year
                SELECT week_of_year
                FROM {table}
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgs) DESC
                LIMIT 1;
            """).fetchone()[0]
            
            # 4. Carbon lightest week of the year
            co2_light_week = con.execute(f"""
                -- Getting the lightest week of the year
                SELECT week_of_year
                FROM {table}
                GROUP BY week_of_year
                ORDER BY AVG(trip_co2_kgs) ASC
                LIMIT 1;
            """).fetchone()[0]
            for cond, val in zip(['HEAVIEST', 'LIGHTEST'], [co2_heavy_week, co2_light_week]):
                print(f"\tThe {cond} carbon week of the year for {color} trips was: {val}")
                logger.info(f"\tThe {cond} carbon week of the year for {color} trips was: {val}")



            # 5. Carbon heaviest month of the year
            co2_heavy_month = con.execute(f"""
                -- Getting the heaviest month of the year
                SELECT month_of_year
                FROM {table}
                GROUP BY month_of_year
                ORDER BY AVG(trip_co2_kgs) DESC
                LIMIT 1;
            """).fetchone()[0]
            
            # 5. Carbon lightest month of the year
            co2_light_month = con.execute(f"""
                -- Getting the lightest month of the year
                SELECT month_of_year
                FROM {table}
                GROUP BY month_of_year
                ORDER BY AVG(trip_co2_kgs) ASC
                LIMIT 1;
            """).fetchone()[0]
            for cond, val in zip(['HEAVIEST', 'LIGHTEST'], [co2_heavy_month, co2_light_month]):
                print(f"\tThe {cond} carbon month of the year for {color} trips was: {val}")
                logger.info(f"\tThe {cond} carbon month of the year for {color} trips was: {val}")    

            

        # 6. Time-series plot (co2 vs year) by taxi color
        yellow_agg = con.execute("""
    SELECT
        EXTRACT(YEAR FROM pickup_time) AS year,
        'yellow' AS color,
        SUM(trip_co2_kgs) AS trip_co2_kgs
    FROM final_yellow_data
    WHERE EXTRACT(YEAR FROM pickup_time) >= 2023
    GROUP BY year
""").df()

        green_agg = con.execute("""
    SELECT
        EXTRACT(YEAR FROM pickup_time) AS year,
        'green' AS color,
        SUM(trip_co2_kgs) AS trip_co2_kgs
    FROM final_green_data
    WHERE EXTRACT(YEAR FROM pickup_time) >= 2023
    GROUP BY year
""").df()

# Combine the aggregated results
        yearly_sum = pd.concat([yellow_agg, green_agg], axis=0, ignore_index=True)

        
        plt.figure(figsize = (12, 8))
        for color in yearly_sum['color'].unique():
            subset = yearly_sum[yearly_sum['color'] == color]
            plt.plot(subset['year'], subset['trip_co2_kgs'], marker='o', label=color, color=color)
        plt.xlabel('Year')
        plt.ylabel('Sum of CO2 Production (Kg)')
        plt.title("Total CO2 Production by Year")
        plt.legend()
        plt.savefig("./co2_timeseries_analysis.png")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")


if __name__ == "__main__":
    analyze()
