import time
import sqlite3
import pandas as pd
import psycopg2
import duckdb
from sqlalchemy import create_engine, text

DATABASE_NAME = 'database.db'
duckdb_df_name = 'trips'

def connect_to_db():
    sqlite_conn = sqlite3.connect(DATABASE_NAME)

    # Подключение к базе данных PostgreSQL
    psycopg2_conn = psycopg2.connect(
        dbname=DATABASE_NAME,
        user="user",
        password="password",
        host="localhost",
    )
    # # Подключение к базе данных DuckDB
    con = duckdb.connect(':memory:')
    con.execute(f"CREATE TABLE {duckdb_df_name} AS SELECT * FROM {duckdb_df_name}")


    # Подключение к базе данных SQLAlchemy
    sqlalchemy_engine = create_engine(f'sqlite:///{DATABASE_NAME}')

    return sqlite_conn, psycopg2_conn, con, sqlalchemy_engine


# Словарь для хранения медианных времен выполнения
median_times = {'SQLite': [], 'PostgreSQL': [], 'DuckDB': [], 'Pandas': [], 'SQLAlchemy': []}

def run_benchmark_for_db(median_times, queries, postgres_queries, sqlite_conn, psycopg2_conn, duckdb_conn,
                         sqlalchemy_engine):
    # Бенчмарк
    for query in queries:
    # SQLite
        execution_times = []
        for _ in range(2):
            print('пошли запросы 1')
            start_time = time.time()
            pd.read_sql_query(query, sqlite_conn)
            end_time = time.time()
            execution_times.append(end_time - start_time)
        median_times['SQLite'].append(sorted(execution_times)[len(execution_times) // 2])

    #     # DuckDB
        execution_times = []
        for _ in range(10):
            start_time = time.time()
            result = duckdb_conn.execute(query).fetchall()
            end_time = time.time()
            execution_times.append(end_time - start_time)
        median_times['DuckDB'].append(sorted(execution_times)[len(execution_times) // 2])

    #     # SQLAlchemy
        execution_times = []
        for _ in range(2):
            print('пошли запросы 2')

            start_time = time.time()
            pd.read_sql_query(text(query), sqlalchemy_engine)
            end_time = time.time()
            execution_times.append(end_time - start_time)
        median_times['SQLAlchemy'].append(sorted(execution_times)[len(execution_times) // 2])

    for query in postgres_queries:
            # PostgreSQL
        execution_times = []
        for _ in range(10):
            with psycopg2_conn.cursor() as cur:
                start_time = time.time()
                cur.execute(query)
                result = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
                end_time = time.time()
                execution_times.append(end_time - start_time)
        median_times['PostgreSQL'].append(sorted(execution_times)[len(execution_times) // 2])

    return median_times



