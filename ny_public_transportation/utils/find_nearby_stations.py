import os
import pandas as pd
import numpy as np
def find_nearby_stations(
    target_coordinates: tuple,
    radius: float,
    kind: str = "citibike"
):
    """
    Find nearby Citi Bike stations given a target location and radius.

    Parameters
    ----------
    target_coordinates : tuple
        The target location (latitude, longitude).
    radius : float
        The radius in meters.
    kind : str
        The type of station to find. Options are 'citibike' or 'subway'.

    Returns
    -------
    list
        A list of nearby Citi Bike stations.
    """

    filename = "citibike_location_corrected.pkl" if kind == "citibike" else "subway_coord.pkl"
    lat_col = "corrected_lat" if kind == "citibike" else "lat"
    lng_col = "corrected_lng" if kind == "citibike" else "lng"
    station_col = "station_name" if kind == "citibike" else "station"

    df = pd.read_pickle(os.path.join(os.path.dirname(__file__), "../data/" + filename))
    df['distance'] = haversine_np(df[lng_col], df[lat_col],
                                  np.repeat(target_coordinates[1], len(df)),
                                    np.repeat(target_coordinates[0], len(df))) * 1000
    df = df[df['distance'] <= radius]
    df = df.sort_values(by='distance').reset_index(drop=True)
    if kind == "citibike":
        df = df.drop(columns=['lat', 'lng'])
    df = df.rename(columns={station_col: 'station', lat_col: 'lat', lng_col: 'lng'})
    return df[['station', 'lat', 'lng', 'distance']]


def haversine_np(lon1, lat1, lon2, lat2):
    """
    ref: https://stackoverflow.com/questions/29545704/fast-haversine-approximation-python-pandas
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.    

    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km