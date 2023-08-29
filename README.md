# New York Public Transit Data Retrieval/Processing Package

This Python package provides a preliminary set of tools for analyzing public transit data in New York City. It provides easy-to-use functions for fetching, preprocessing, and visualizing data from MTA subway turnstiles and Citi Bike stations.

## Features

- Retrieve data from New York State Open Data API for MTA subway turnstiles and Citi Bike stations.
- Preprocess the data to a standard format.
- Analyze the data for insights, such as calculating the rolling percent change in ridership.
- Visualize the results in a plot.
- Perform geospatial analysis to find nearby Subway/Citi Bike stations given a target location and radius.

## Installation

You may clone this repository and import the modules directly into your Python environment. Please note that this package requires Python 3.6 or above.

```bash
git clone https://github.com/yourusername/ny_public_transportation.git
```

## Usage

Before using this package, you need to set the API keys for Open Data and Mapbox. This can be done with the following functions:

- Note: This is only necessary if you wish to use MTA Open API to retrieve subway data. Mapbox token is used to provide mapbox map-based visualizations.

```python
from ny_public_transportation import APIKeys

APIKeys.set_open_data_api_key('your_open_data_api_key')
APIKeys.set_mapbox_api_key('your_mapbox_api_key')
```

After setting the API keys, you can use the package's functions to retrieve and analyze data. Below are a few examples:

```python
import ny_public_transportation as npt
import pandas as pd

START_DATE = "2020-03-01"
END_DATE = "2020-04-30"
STATION = "CATHEDRAL PKWY"
# Retrieve MTA data for the year 2020
df_subway = npt.retrieve_mta_data_as_df([2020], date_range=(START_DATE, END_DATE), station=STATION)

# Process the MTA data into counts
df_subway = npt.turnstile_to_counts(df_subway, granularity="4H", detail_level="station", interpolate=True)

# Retrieve and process Citibike data
df_citibike = npt.retrieve_citibike_data_as_df(
    start_date=pd.to_datetime(START_DATE).strftime("%Y%m"),
    end_date=(pd.to_datetime(END_DATE) + pd.Timedelta(days=31)).strftime("%Y%m")
)
df_citibike = npt.trips_to_counts(df_citibike, granularity="4H", interpolate=True)
```

Please refer to the docstrings in each module and function for more detailed usage instructions.
