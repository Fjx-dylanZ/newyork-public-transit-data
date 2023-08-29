import pandas as pd
import pandas as pd

def generate_where_statement(stations: list = None, date_range: tuple = None) -> str:
    """
    Generate the WHERE statement for SoQL API query.
    # Example usage
        generate_where_statement(stations=["CATHEDRAL PKWY", "103 ST"], date_range=("2020-01-01", "2020-04-01"))

    Parameters
    ----------
    stations : list, optional
        A list of stations to query.
    date_range : tuple, optional
        A tuple of (start_date, end_date) to query.
        
    Returns
    -------
    str
        The WHERE statement for the SoQL API query.
    """
    
    # Check if both parameters are None
    if not stations and not date_range:
        return None
    
    conditions = []

    # Handle date range condition
    if date_range:
        start_date, end_date = [pd.to_datetime(date).strftime('%Y-%m-%dT00:00:00') for date in date_range]
        conditions.append(f'`date` BETWEEN "{start_date}"::floating_timestamp AND "{end_date}"::floating_timestamp')

    # Handle stations condition
    if stations:
        quoted_stations = ', '.join([f'"{station}"' for station in stations])
        conditions.append(f'caseless_one_of(`station`, {quoted_stations})')

    return ' AND '.join(conditions)


