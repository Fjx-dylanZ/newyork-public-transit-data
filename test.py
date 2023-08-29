import ny_public_transportation as npt
import os


if __name__ == "__main__":
    npt.set_open_data_api_key(open(".opendatatoken").read().strip())
    print(npt.retrieve_mta_data_as_df([2020], station=None))
#     print(npt._retrieve_mta_data_as_df([2020], station="CATHEDRAL PKWY"))
#     #print(npt.retrieve_mta_data_as_df(2020, station="CATHEDRAL PKWY"))
    #df = npt.retrieve_mta_data_as_df([2020, 2021], station="CATHEDRAL PKWY")
    #print(df)
    #print(npt.turnstile_to_counts(df, granularity='4H', detail_level='station', interpolate=True))
    #print(npt.retrieve_citibike_data_as_df(start_date=202002))
    # print(npt.trips_to_counts(
    #     npt.retrieve_citibike_data_as_df(
    #         start_date=202002, end_date=202003), granularity='4H', interpolate=True).sort_values(by='datetime_rounded', ascending=False))
    #print(npt.find_nearby_stations((40.804213, -73.966991), 1000, kind='citibike'))

    # df = npt.retrieve_mta_data_as_df([2020, 2021], station="CATHEDRAL PKWY")
    # df = npt.turnstile_to_counts(df, granularity='4H', detail_level='station', interpolate=True)
    # df = npt.get_rolling_pct_change(df, window='7D', cols=['entries_diff', 'exits_diff'])
    # import matplotlib.pyplot as plt
    # df.plot(x='datetime_rounded', y=['entries_diff', 'exits_diff'])
    # plt.show()
    # print(df)

    # df = npt.retrieve_citibike_data_as_df(start_date=202001, end_date=202004)
    # df = npt.trips_to_counts(df, granularity='4H', interpolate=True)
    # df = df.loc[(df['citibike_station'] == 'Cathedral Pkwy & Broadway') | (df['citibike_station'] == 'W 106 St & Amsterdam Ave')]
    # df = npt.get_rolling_pct_change(df, window='7D', cols=['start_count', 'end_count'], groupby_key='citibike_station')
    # print(df)