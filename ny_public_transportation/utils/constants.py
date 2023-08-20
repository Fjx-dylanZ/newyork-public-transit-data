class APIKeys:
    _opendata_api_key = None
    _mapbox_api_key = None
    _gcp_api_key = None

    @classmethod
    def set_open_data_api_key(cls, key):
        cls._opendata_api_key = key

    @classmethod
    def get_open_data_api_key(cls):
        return cls._opendata_api_key

    @classmethod
    def set_mapbox_api_key(cls, key):
        cls._mapbox_api_key = key

    @classmethod
    def get_mapbox_api_key(cls):
        return cls._mapbox_api_key

    @classmethod
    def set_gcp_api_key(cls, key):
        cls._gcp_api_key = key

    @classmethod
    def get_gcp_api_key(cls):
        return cls._gcp_api_key

MTA_ENDPOINTS = {
    2017: "https://data.ny.gov/resource/v5y5-mwpb.json",
    2018: "https://data.ny.gov/resource/bjcb-yee3.json",
    2019: "https://data.ny.gov/resource/xfn5-qji9.json",
    2020: "https://data.ny.gov/resource/py8k-a8wg.json",
    2021: "https://data.ny.gov/resource/uu7b-3kff.json",
    2022: "https://data.ny.gov/resource/k7j9-jnct.json"
}

CITIBIKE_DATASET_URL = "https://s3.amazonaws.com/tripdata"