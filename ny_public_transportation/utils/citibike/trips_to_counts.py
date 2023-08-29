import pandas as pd
import numpy as np
from itertools import product

def trips_to_counts(df_citibike, granularity='4H', interpolate=True):
    """
    Convert a dataframe of Citi Bike trips to a dataframe of Citi Bike counts.
    """
    citibike_stations = np.hstack([df_citibike['start_station_name'].unique(), df_citibike['end_station_name'].unique()])
    citibike_stations = np.unique(citibike_stations)

    # df of all possible combinations of station and datetime
    min_time = df_citibike['started_at'].min().floor('4H')
    max_time = df_citibike['ended_at'].max().ceil('4H')
    print(min_time, max_time)
    df_all = (
        pd.DataFrame(
            product(citibike_stations, pd.date_range(min_time, max_time, freq=granularity)),
            columns=['citibike_station', 'datetime_rounded']
        )
    )

    df_citibike_start = (
        df_citibike
        .assign(started_at=lambda x: pd.to_datetime(x.started_at).dt.floor('4H'))
        .groupby(['started_at', 'start_station_name'])
        .size()
        .reset_index(name='start_count')
    )
    df_citibike_end = (
        df_citibike
        .assign(ended_at=lambda x: pd.to_datetime(x.ended_at).dt.floor('4H'))
        .groupby(['ended_at', 'end_station_name'])
        .size()
        .reset_index(name='end_count')
    )
    return (
        df_all
        .merge(df_citibike_start, how='left', left_on=['datetime_rounded', 'citibike_station'], right_on=['started_at', 'start_station_name'])
        .drop(columns=['started_at', 'start_station_name'])
        .merge(df_citibike_end, how='left', left_on=['datetime_rounded', 'citibike_station'], right_on=['ended_at', 'end_station_name'])
        .drop(columns=['ended_at', 'end_station_name'])
        .fillna(0)
    ).sort_values(by=['citibike_station', 'datetime_rounded']) #TODO: double check the implementation here
