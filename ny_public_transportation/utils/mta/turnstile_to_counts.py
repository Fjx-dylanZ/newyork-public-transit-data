import pandas as pd
import numpy as np

def turnstile_to_counts(df_subway, granularity='4H'):
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

    Returns
    -------
    pandas.DataFrame
    """
    df_subway = df_subway.copy()
    