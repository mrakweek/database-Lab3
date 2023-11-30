import time
import pandas as pd

def run_benchmark_for_pandas(df, median_times):
    for query_label in ['Q1', 'Q2', 'Q3', 'Q4']:
        execution_times = []
        for _ in range(10):
            start_time = time.time()

            if query_label == 'Q1':
                selected_df = df[['VendorID']]
                grouped_df = selected_df.groupby('VendorID')
                final_df = grouped_df.size().reset_index(name='counts')
            elif query_label == 'Q2':
                selected_df = df[['passenger_count', 'total_amount']]
                grouped_df = selected_df.groupby('passenger_count')
                final_df = grouped_df.mean().reset_index()
            elif query_label == 'Q3':
                selected_df = df[['passenger_count', 'tpep_pickup_datetime']]
                selected_df['year'] = pd.to_datetime(
                    selected_df.pop('tpep_pickup_datetime'),
                    format='%Y-%m-%d %H:%M:%S').dt.year
                grouped_df = selected_df.groupby(['passenger_count', 'year'])
                final_df = grouped_df.size().reset_index(name='counts')
            elif query_label == 'Q4':
                selected_df = df[[
                    'passenger_count',
                    'tpep_pickup_datetime',
                    'trip_distance']]
                selected_df['trip_distance'] = selected_df['trip_distance'].round().astype(int)
                selected_df['year'] = pd.to_datetime(
                    selected_df.pop('tpep_pickup_datetime'),
                    format='%Y-%m-%d %H:%M:%S').dt.year
                grouped_df = selected_df.groupby([
                    'passenger_count',
                    'year',
                    'trip_distance'])
                final_df = grouped_df.size().reset_index(name='counts')
                final_df = final_df.sort_values(
                    ['year', 'counts'],
                    ascending=[True, False])

            end_time = time.time()
            execution_times.append(end_time - start_time)

        median_times['Pandas'].append(sorted(execution_times)[len(execution_times) // 2])

    return median_times