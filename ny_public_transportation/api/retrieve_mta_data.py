import pandas as pd
import requests
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import tqdm
import time
import warnings
from .retrieve_mta_data_legacy import retrieve_mta_data_as_df as _retrieve_mta_data_as_df
from ..utils.constants import APIKeys, MTA_ENDPOINTS
from ..utils.mta.mta_preprocess import mta_preprocess
from ..utils.mta.generate_query_condition import generate_where_statement

def retrieve_mta_data_page(args):
    url, headers, params = args

    ### TIME ###
    _start = time.time()

    response = requests.get(url, params=params, headers=headers)

    _end = time.time()
    print(f"Fold {params['$offset']} finished in {_end - _start} seconds.")
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print("Error: " + str(response.status_code))
        return None
    
def retrieve_mta_data(
        year,
        limit=1000000,
        query=None,
        station=None,
        date_range=None,
        parallel=True,
        n_threads=6,
):

    try:
        url = MTA_ENDPOINTS[year]
    except KeyError:
        raise ValueError(f"Invalid year: {year}")
    # retrieve API key
    OPENDATA_API_KEY = APIKeys.get_open_data_api_key()
    if OPENDATA_API_KEY is None:
        raise ValueError("Open Data API key not set.")
    
    # TODO: implement standardized query formatter
    order = "`date` DESC NULL FIRST, `time` ASC NULL LAST, `c_a` ASC NULL LAST, `unit` ASC NULL LAST"
    offset = 0
    headers = {'X-App-Token': OPENDATA_API_KEY}
    #where = f"`station` = '{station}'" if station else None
    if date_range:
        # check if date_range is within the year
        _extract_year = lambda date: pd.to_datetime(date).year
        start_year, end_year = [_extract_year(date) for date in date_range]
        if start_year != year or end_year != year:
            raise ValueError(f"date_range must be within the year {year}.")
    if station:
        if not isinstance(station, list):
            station = [station]
        station = [s.upper() for s in station]
    where = generate_where_statement(stations=station, date_range=date_range)
    
    json_data = []
    if not parallel:
        warnings.warn("Non-parallel version does not support date_range, and only supports one station at a time.")
        return _retrieve_mta_data_as_df(year, limit, query, station, date_range)
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        while True:
            futures = []
            params = {'$limit': limit, '$offset': offset, '$order': order, '$query': query, '$where': where}
            params = {k: v for k, v in params.items() if v is not None}
            params_list = [params.copy() for _ in range(n_threads)]
            for i in range(n_threads):
                params_list[i]['$offset'] = offset + i * limit
                futures.append(executor.submit(retrieve_mta_data_page, (url, headers, params_list[i])))
            offset += n_threads * limit

            no_data = []
            for future in as_completed(futures):
                data = future.result()
                if not data or len(data) == 0:
                    no_data.append(True)
                json_data.extend(data)
                no_data.append(False)

            if any(no_data):
                break
            print(f"Retrieved {offset} rows.")

    return json_data

def retrieve_mta_data_as_df(
        year,
        limit=1500000,
        query=None,
        station=None,
        date_range=None,
        parallel=True,
        n_threads=6,
):
    """
    Retrieve MTA data from the New York State Open Data API.
    :param year: The year(s) of the data to retrieve.
    :param limit: The maximum number of rows to retrieve.
    :param query: A SQL query to filter the data.
    :param station: The station(s) to retrieve data for.
    :param date_range: A tuple of (start_date, end_date) to retrieve data for.
    :return: A pandas DataFrame containing the MTA data.
    """
    if len(year) == 1: return mta_preprocess(pd.DataFrame(retrieve_mta_data(year[0], limit, query, station, date_range, parallel, n_threads)))
    df = pd.concat(
        [pd.DataFrame(
            retrieve_mta_data(y, limit, query, station, date_range, parallel, n_threads)
         ) for y in year]
        )
    return mta_preprocess(df)