from ..constants import CITIBIKE_DATASET_URL
from .scrape_filelist import retrieve_file_urls
import requests
from xml.etree import ElementTree
import re
import pandas as pd
import zipfile
import os
import io
import tqdm

def process_filename_df():
    """
    function that downloads the citibike dataset file/url list and processes it into a dataframe
    """
    df_files = retrieve_file_urls()
    get_start_end = (
        lambda filename, regex=re.compile(r'^(?:JC-)?(?P<start>\d+)(?:-(?P<end>\d+))?'):
        regex.match(filename).group('start', 'end')
    )

    df_files['start'], df_files['end'] = zip(*df_files['filename'].map(get_start_end))
    df_files['start'] = pd.to_datetime(df_files['start'], format='%Y%m')
    df_files['end'] = pd.to_datetime(df_files['end'], format='%Y%m')
    df_files['is_JC'] = df_files['filename'].str.startswith('JC-') # 'JC-' prefix indicates Jersey City
    df_files = df_files.sort_values(by=['start', 'end']).reset_index(drop=True)
    return df_files

def download_and_load_citibike_data(url):
    r = requests.get(url, stream=True)
    zip_data = io.BytesIO()
    for chunk in r.iter_content(chunk_size=1024):
        if chunk:
            zip_data.write(chunk)
    zip_data.seek(0) # move to beginning of file

    with zipfile.ZipFile(zip_data) as z:
        csv_filename = [f for f in z.namelist() if f.endswith('.csv') and not f.startswith('__MACOSX')]
        if len(csv_filename) != 1:
            raise ValueError(f'Expected 1 csv file, found {len(csv_filename)}. File list: {csv_filename}')
        with z.open(csv_filename[0]) as csv_file:
            df_return = pd.read_csv(csv_file)
    
    return df_return

def process_citibike_data(df_in):
    """
    Preprocess citibike data to fix column names and station names
    Official releases of citibike data have inconsistent column names that change over time.
    """
    df_in = _fix_col_names(df_in)
    df_in = _fix_citibike_station_names(df_in)
    return df_in

def _fix_col_names(df_in):
    '''unify column names to lowercase with underscores, and select only columns of interest'''
    df_in.columns = [c.replace(' ', '_').lower() for c in df_in.columns]
    target_names = {
        'started_at': ['starttime', 'start_time', 'started_at'],
        'ended_at': ['stoptime', 'stop_time', 'ended_at'],
        'start_lat': ['start_station_latitude', 'start_lat'],
        'start_lng': ['start_station_longitude', 'start_lng'],
        'end_lat': ['end_station_latitude', 'end_lat'],
        'end_lng': ['end_station_longitude', 'end_lng'],
        'start_station_name': ['start_station_name'],
        'end_station_name': ['end_station_name'], # add more as needed, key is the desired name, value is a list of possible names
    }

    rename_dict = {}
    for target_name, possible_names in target_names.items():
        for possible_name in possible_names:
            if possible_name in df_in.columns:
                rename_dict[possible_name] = target_name
                break
    df_in = df_in.rename(columns=rename_dict)
    return df_in[list(target_names.keys())]

def _fix_citibike_station_names(df_in):
    return (
        df_in
        .assign(
            start_station_name = lambda x: x['start_station_name'].str.replace(r"\\t|\t|\\r|\r|\\n|\n", " ", regex=True).str.strip(),
            end_station_name = lambda x: x['end_station_name'].str.replace(r"\\t|\t|\\r|\r|\\n|\n", " ", regex=True).str.strip()
        )
    )

