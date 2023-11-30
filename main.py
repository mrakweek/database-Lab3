import pandas as pd
from db_queries import run_benchmark_for_db, connect_to_db
from pandas_queries import run_benchmark_for_pandas
from plot_graphs import plot_barplots

trips = pd.read_csv('nyc_yellow_tiny.csv')
trips = trips.drop('airport_fee', axis=1)
trips = trips.drop('Unnamed: 0', axis=1)
trips['tpep_dropoff_datetime'] = pd.to_datetime(trips['tpep_dropoff_datetime'])

sqlite_conn, psycopg2_conn, duckdb_conn, sqlalchemy_engine = connect_to_db()

# Запросы
queries = [
    "SELECT VendorID, count(*) FROM trips GROUP BY 1;""",
    "SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;",
    "SELECT passenger_count, strftime('%Y', tpep_dropoff_datetime) as dropoff_year, count(*) FROM trips GROUP BY 1, 2;",
    "SELECT passenger_count, strftime('%Y', tpep_dropoff_datetime) as dropoff_year, round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;"
]

postgres_queries = [
    "SELECT VendorID, count(*) FROM trips GROUP BY 1;",
    "SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;",
    "SELECT passenger_count, TO_CHAR(tpep_dropoff_datetime, 'YYYY') as dropoff_year, count(*) FROM trips GROUP BY 1, 2;",
    "SELECT passenger_count, TO_CHAR(tpep_dropoff_datetime, 'YYYY') as dropoff_year, round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;"

]
pandas_queries = [
    trips.groupby('VendorID').size().reset_index(name='count'),  # Query 1
    trips.groupby('passenger_count')['total_amount'].mean().reset_index(name='avg_total_amount'),  # Query 2
    trips.assign(dropoff_year=trips['tpep_dropoff_datetime'].dt.year).groupby(
        ['passenger_count', 'dropoff_year']).size().reset_index(name='count'),  # Query 3
    trips.assign(
        dropoff_year=trips['tpep_dropoff_datetime'].dt.year,
        rounded_trip_distance=trips['trip_distance'].round()
    ).groupby(['passenger_count', 'dropoff_year', 'rounded_trip_distance']).size().reset_index(
        name='count').sort_values(by=['dropoff_year', 'count'], ascending=[True, False])  # Query 4
]

median_times = {'SQLite': [], 'PostgreSQL': [], 'DuckDB': [], 'Pandas': [], 'SQLAlchemy': []}

median_times = run_benchmark_for_db(median_times, queries, postgres_queries, sqlite_conn, psycopg2_conn, duckdb_conn,
                         sqlalchemy_engine)
median_times = run_benchmark_for_pandas(trips, median_times)

plot_barplots(median_times, queries)

