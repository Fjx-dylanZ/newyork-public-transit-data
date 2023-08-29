import pandas as pd

def mta_preprocess(df_subway):
    df_subway['date'] = df_subway['date'].apply(lambda x: x[:10])
    df_subway['datetime'] = pd.to_datetime(df_subway['date'] + ' ' + df_subway['time'],
                                             format='%Y-%m-%d %H:%M:%S')
    
    # Convert to int
    df_subway['entries'] = df_subway['entries'].astype(int)
    df_subway['exits'] = df_subway['exits'].astype(int)
    
    # Drop duplicates
    df_subway = df_subway.drop_duplicates()
    
    return df_subway
    # return (
    #     df_subway
    #     .assign(datetime=lambda x: pd.to_datetime(x['date'].str.split('T').str[0] + ' ' + x['time']))
    #     .assign(entries=lambda x: x['entries'].astype(int))
    #     .assign(exits=lambda x: x['exits'].astype(int))
    #     .drop_duplicates()
    # )