import pandas as pd

def get_rolling_pct_change(df, window: str = "7D", datetime_col: str = "datetime_rounded",
                           cols: list = None, groupby_key: str = None):
    """
    Calculate the rolling percent change of a dataframe.

    Parameters
    ----------
    df : pandas.DataFrame
        A dataframe of data.
    window : str
        Pandas time series frequency string. Default is '7D' (7 days).
    cols : list
        A list of columns to calculate rolling percent change on.
    groupby_key : str
        The column to group by. Default is None (Assuming only one group).

    Returns
    -------
    pandas.DataFrame
    """
    df = df.copy()
    if cols is None:
        cols = df.columns[~df.columns.isin([datetime_col, groupby_key])]

    if groupby_key is not None:
        # Group by the specified key
        assert isinstance(groupby_key, str), "groupby_key must be a string."
        grouped = df.groupby(groupby_key)
    else:
        # If no groupby_key is given, create a single group
        grouped = [("", df)]
        
    result_dfs = []
    for group, group_df in grouped:
        group_df = group_df.set_index(datetime_col).resample(window)
        agg_dict = {c: 'sum' for c in cols}

        # count here is needed to make sure the rolling window is full
        identifier_col = list(agg_dict.keys())[0]
        agg_dict[identifier_col] = ['sum', 'count']
        group_df = group_df.agg(agg_dict)
        group_df.columns = group_df.columns.map('_'.join) # flatten the multi-index

        count_col = identifier_col + '_count'
        desired_count = group_df[count_col].max()
        # get rid of the windows that are not full
        group_df = group_df.loc[group_df[count_col] == desired_count, :].drop(count_col, axis=1)
        group_df.columns = group_df.columns.str.replace('_sum', '') # restore the column names

        # calculate the rolling percentage change
        group_df.loc[:, cols] = group_df.loc[:, cols].pct_change()
        group_df = group_df.reset_index().dropna()

        # Append the groupby_key to the result if it exists
        if groupby_key is not None:
            group_df[groupby_key] = group

        result_dfs.append(group_df)

    # Concatenate the results and return
    result_df = pd.concat(result_dfs, axis=0, ignore_index=True)
    return result_df
