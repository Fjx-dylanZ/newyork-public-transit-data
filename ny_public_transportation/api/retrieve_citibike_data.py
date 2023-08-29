import numpy as np 
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import tqdm


from ..utils.citibike.retrieve_backend import process_filename_df, download_and_load_citibike_data, process_citibike_data

def retrieve_citibike_data_single_month(date, df_files):
    """
    params:
        date: YYYYMM
        df_files: dataframe of citibike file urls
    """
    try:
        date = pd.to_datetime(date, format='%Y%m')
    except ValueError:
        raise ValueError("Incorrect date format, should be YYYYMM")
    
    # we currently ignore jersey city data
    download_url = df_files.loc[(df_files['start'] == date) & (df_files['is_JC'] == False), 'url'].values[0]
    if download_url is None:
        raise ValueError(f'No data available for {date}')
    print(f'Downloading {download_url}')

    df_return = download_and_load_citibike_data(download_url)
    df_return = process_citibike_data(df_return)
    return df_return

def retrieve_citibike_data_as_df(start_date, end_date=None):
    if end_date is None:
        end_date = start_date
    try:
        start_date = pd.to_datetime(start_date, format='%Y%m')
        end_date = pd.to_datetime(end_date, format='%Y%m')
    except:
        print("Invalid date format")
        return
    assert start_date <= end_date, "Start date must be before end date"
    date_range = pd.date_range(start_date, end_date, freq='MS')
    print(f"Retrieving data from {','.join([d.strftime('%Y%m') for d in date_range])}")

    # download and process data
    df_files = process_filename_df()
    dfs = []

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(retrieve_citibike_data_single_month, date, df_files): date for date in date_range}

        for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
            try:
                dfs.append(future.result())
            except Exception as e:
                print(f"Failed to retrieve data for {futures[future]}: {e}")

    # Combine the results
    df = pd.concat(dfs, ignore_index=True)
    df = df.assign(
            started_at=lambda df: pd.to_datetime(df['started_at'].str.split('.').str[0]), # remove milliseconds
            ended_at=lambda df: pd.to_datetime(df['ended_at'].str.split('.').str[0]), 
        )
    return df
