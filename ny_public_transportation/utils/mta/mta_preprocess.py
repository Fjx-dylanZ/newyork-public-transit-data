import pandas as pd

def mta_preprocess(df_subway):
    return (
        df_subway
        .assign(datetime=lambda x: pd.to_datetime(x['date'].str.split('T').str[0] + ' ' + x['time']))
        .assign(entries=lambda x: x['entries'].astype(int))
        .assign(exits=lambda x: x['exits'].astype(int))
        .drop_duplicates()
    )