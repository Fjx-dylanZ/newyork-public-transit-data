import pandas as pd
import numpy as np

def turnstile_to_counts(df_subway, granularity='4H', interpolate=True, detail_level='station'):
    """
    This function takes a dataframe of MTA subway turnstile data and returns a dataframe with \
    the number of entries and exits for each turnstile for each day. 

    **Warning**: This function does not account for irregularities in turnstile data collection. \
             For example, if the granularity is set to '4H', the function aggregates the entries \
             and exits for each turnstile for each 4 hour period (0:00, 4:00, 8:00, 12:00, 16:00, 20:00). \
             If the data collection for a particular turnstile is not consistent with this granularity, \
             the resulting dataframe will not be accurate. It is recommended to use larger granularities \
             (e.g. '1D') to minimize the effect of irregularities.

    Parameters
    ----------
    df_subway : pandas.DataFrame
        A dataframe of MTA subway turnstile data.
    granularity : str
        Pandas time series frequency string. Default is '4H' (4 hours).
    interpolate : bool
        Whether to interpolate missing values. Default is True.
    detail_level : str
        The level of detail to return. Default is 'station'. Options are 'station', 'turnstile'

    Returns
    -------
    pandas.DataFrame
    """
    df_subway = df_subway.copy()
    df_subway['datetime_rounded'] = pd.to_datetime(df_subway['datetime']).dt.round(granularity)

    group_keys = ['c_a', 'unit', 'scp', 'station', 'line_name', 'division']
    min_time = df_subway['datetime_rounded'].min()
    max_time = df_subway['datetime_rounded'].max() + pd.Timedelta(granularity) / 2
    
    # ReIndex, fill missing datetimes with NaNs
    df_subway = df_subway.groupby(group_keys).apply(reindex_by_group, min_time, max_time)
    df_subway = df_subway.reset_index(drop=True)

    # Calculate entries_diff and exits_diff
    df_subway = df_subway.groupby(group_keys).apply(diff_count)
    df_subway = df_subway.reset_index(drop=True)

    # Interpolate missing values
    if interpolate:
        df_subway = df_subway.groupby(group_keys).apply(interpolate_by_group)
        df_subway = df_subway.reset_index(drop=True)

    # rename
    if detail_level == 'turnstile':
        return df_subway
    elif detail_level == 'station':
        df_subway = df_subway.groupby(['station', 'datetime_rounded'])
        df_subway = df_subway.agg({'entries_diff': 'sum', 'exits_diff': 'sum'})
        df_subway = df_subway.reset_index()
        return df_subway
    else:
        raise ValueError("detail_level must be either 'turnstile' or 'station'.")


def name_group(group):
    group['group_name'] = "_".join(list(group.name))
    return group

def diff_count(group):
    THEORETICAL_MAX_FLOW = 10000 # 99th percentile of entries_diff and exits_diff is ~ 5000
    MAX_TIME_DIFF = pd.Timedelta('1 day')
    group = group.sort_values(by='datetime_rounded')
    group['entries_diff'] = group['entries'].diff()
    group['exits_diff'] = group['exits'].diff()
    group['time_diff'] = group['datetime_rounded'].diff().fillna(pd.Timedelta(seconds=0)) # only the first row will have a NaT
    group['entries_diff'] = np.where((group['entries_diff'] < 0) | (group['entries_diff'] > THEORETICAL_MAX_FLOW) | (group['time_diff'] > MAX_TIME_DIFF), np.nan, group['entries_diff'])
    group['exits_diff'] = np.where((group['exits_diff'] < 0) | (group['exits_diff'] > THEORETICAL_MAX_FLOW) | (group['time_diff'] > MAX_TIME_DIFF), np.nan, group['exits_diff'])

    return group

def reindex_by_group(group, global_start, global_end):
    group = group.drop_duplicates(subset=['datetime_rounded'], keep='first')
    date_index = pd.date_range(global_start, global_end, freq='4H')
    group = group.set_index('datetime_rounded')
    group = group.reindex(date_index)
    group['datetime_rounded'] = group.index
    #print(group.isna().any(axis=1).sum())
    
    # fill in missing values due to reindexing
    index_na = group.index[group.isna().any(axis=1)]
    group = group.fillna(method='ffill')
    group.loc[index_na, ['entries', 'exits', 'datetime', 'date', 'time']] = np.nan
    #print(group.loc[group.isna().any(axis=1)])
    #print(group.isna().any(axis=1).sum())
    return group

def interpolate_by_group(group):
    group = group.set_index('datetime_rounded')
    group['entries_diff'] = group['entries_diff'].interpolate(method='time')
    group['exits_diff'] = group['exits_diff'].interpolate(method='time')
    group['datetime_rounded'] = group.index
    return group