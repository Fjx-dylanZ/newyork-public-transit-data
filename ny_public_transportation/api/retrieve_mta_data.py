import pandas as pd
import requests
import requests
import numpy as np 
from ..utils.constants import APIKeys, MTA_ENDPOINTS

def retrieve_mta_data(
        year,
        limit=1000000,
        query=None,
        station=None,
        date_range=None,
):
    try:
        url = MTA_ENDPOINTS[year]
    except KeyError:
        raise ValueError(f"Invalid year: {year}")
    # retrieve API key
    OPENDATA_API_KEY = APIKeys.get_open_data_api_key()
    if OPENDATA_API_KEY is None:
        raise ValueError("Open Data API key not set.")
    
    order = "`date` DESC NULL FIRST, `time` ASC NULL LAST, `c_a` ASC NULL LAST, `unit` ASC NULL LAST"
    offset = 0
    limit = 1000000 # 1m per request
    headers = {'X-App-Token': OPENDATA_API_KEY}
    where = f"`station` = '{station}'" if station else None
    if date_range:
        date_range = None
        raise NotImplementedError("Date range not implemented yet.")
    
    json_data = []
    while True:
        params = {'$limit': limit, '$offset': offset, '$order': order, '$query': query, '$where': where}
        params = {k: v for k, v in params.items() if v is not None}
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if not data:
                break
            json_data.extend(data)
            offset += limit
            print(f"Retrieved {offset} rows.")
        else:
            print("Error: " + str(response.status_code))
            break
    
    return json_data

def retrieve_mta_data_as_df(
        year,
        limit=1000000,
        query=None,
        station=None,
        date_range=None
):
    """
    Retrieve MTA data from the New York State Open Data API.
    :param year: The year(s) of the data to retrieve.
    :param limit: The maximum number of records to retrieve on a single API call.
    :param query: A query to filter the data.
    :param station: A station to filter the data by.
    :param date_range: A list of two dates to filter the data by.
    :return: A pandas DataFrame containing the retrieved data.
    """
    
    if len(year) == 1: return pd.DataFrame(retrieve_mta_data(year[0], limit, query, station, date_range))
    df = pd.concat(
        [pd.DataFrame(
            retrieve_mta_data(y, limit, query, station, date_range)
         ) for y in year]
        )
    return df

