#ref:https://daymet.ornl.gov/single-pixel/
# df = pd.DataFrame([[44.509943, -65.00],[44.509943, -65.00]],columns=['lat','lng']); df.T.to_json(); 
json_str='{"0":{"lat":44.509943,"lng":-85.0},"1":{"lat":43.509943,"lng":-85.0}}'
@fused.udf 
def udf(latlngs_json=json_str, start_year=2021, end_year=2023):
    # utils = fused.public.utils
    import utils
    def get_timeseries(lonlat, start_year=start_year, end_year=end_year): 
        try:
            py_url='https://raw.githubusercontent.com/bluegreen-labs/daymetpy/master/daymetpy/daymetpy.py'
            daymet_timeseries = utils.read_module(py_url)['daymet_timeseries']
            df=daymet_timeseries(lon=lonlat[0], lat=lonlat[1], start_year=start_year, end_year=end_year)
            df['lng'] = lonlat[0]
            df['lat'] = lonlat[1]
            return df
        except:
            pass
    # @fused.cache
    def async_get_timeseries(latlngs_json=json_str, start_year=2022, end_year=2023):
        import pandas as pd
        df=pd.read_json(latlngs_json).T
        lonlats = df[['lng','lat']].values
        a = utils.run_async(get_timeseries, arr_args=lonlats)
        # a = fused.public.utils.run_async(get_timeseries, arr_args=lonlats)
        a=[i for i in a if i is not None]
        if len(a)>0:
            df = pd.concat(a)
            gdf = fused.utils.geo_convert(df)
            return gdf 
        else:
            print('no data')
            return None 
    gdf = async_get_timeseries(latlngs_json=latlngs_json, start_year=start_year, end_year=end_year)
    print(gdf)
    return gdf