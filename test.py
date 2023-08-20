import ny_public_transportation as npt
import os
npt.set_open_data_api_key(open(".opendatatoken").read().strip())

print(npt.retrieve_mta_data_as_df([2020, 2021], station="CATHEDRAL PKWY"))
print(npt.retrieve_citibike_data_as_df(202001, 202012))