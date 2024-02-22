#TODO make sure do not cache the future
# https://noaa-goes18.s3.amazonaws.com/index.html#ABI-L1b-RadC/2024/001/00/
@fused.udf   
def udf(partition_str='{"x_start":{"0":1190,"1":2380,"2":3570,"3":1190,"4":2380,"5":3570},"x_stop":{"0":2390,"1":3580,"2":4770,"3":2390,"4":3580,"5":4770},"y_start":{"0":0,"1":0,"2":0,"3":1190,"4":1190,"5":1190},"y_stop":{"0":1200,"1":1200,"2":1200,"3":2390,"4":2390,"5":2390},"fused_index":{"0":0,"1":1,"2":2,"3":3,"4":4,"5":5}}', 
                roi_wkt = '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[-4100000.0, 2400000.0], [3200000.0, 2400000.0], [3200000.0, -1500000.0], [-4100000.0, -1500000.0], [-4100000.0, 2400000.0]]]}}]}',
                crs = 'PROJCRS["WGS84 / Lambert_Conformal_Conic_2SP",BASEGEOGCRS["WGS84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1,ID["EPSG",9001]]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]]],CONVERSION["unnamed",METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],PARAMETER["Latitude of false origin",33,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]],PARAMETER["Longitude of false origin",-125,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Latitude of 1st standard parallel",21,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",45,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Easting at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8827]]],CS[Cartesian,2],AXIS["easting",east,ORDER[1],LENGTHUNIT["Meter",1]],AXIS["northing",north,ORDER[2],LENGTHUNIT["Meter",1]]]',
                i=10, product_name='ABI-L2-CMIPF', bucket_name='noaa-goes18', datestr='2024-01-31', offset='0', band=8, x_res=4000, y_res=4000, min_pixel_value=1500, max_pixel_value=2500, colormap='Blues'): 
    i=int(i);band=int(band);x_res=int(x_res);y_res=int(y_res);
    chunk_size = (1200, 1200)
    min_max=(int(min_pixel_value),int(max_pixel_value))
    import geopandas as gpd
    from shapely.geometry import Polygon 
    import json
    json_data = json.loads(roi_wkt)
    roi = gpd.GeoDataFrame(geometry=[Polygon(feature["geometry"]["coordinates"][0]) for feature in json_data["features"]]
                               ,crs=crs)
    roi['partition_str']=partition_str


    
    #chunk & reproject data
    import pandas as pd
    from utils import file_of_day, chunkify
    product_name, datestr, offset, band, x_res, y_res
    file_path = file_of_day(i, bucket_name, product_name, band, datestr,offset) 
    filename = file_path.split('/')[-1]
    url = f'https://{bucket_name}.s3.amazonaws.com/{file_path}'
    path = fused.utils.download(url,'geos18/'+filename)
    partition_str = roi['partition_str'].iloc[0]
    df = pd.read_json(partition_str)
    # xy_size = (len(xds.x),len(xds.y))
    # slice_list = chunkify(chunk_size=chunk_size, xy_size=xy_size, pad=10)
    # valid_list = cache_valid_list(roi, product_name, bucket_name, chunk_size)
    slice_list = valid_list = list(df.apply(lambda row: [slice(row['x_start'],row['x_stop']), slice(row['y_start'],row['y_stop'])],1).values)
    xy_res=(x_res,y_res)

    @fused.cache
    def fn(path, roi, valid_list, xy_res):
        from utils import roi_ds_chunks, combine_resample        
        import rioxarray
        xds = rioxarray.open_rasterio(path)
        ds_chunks = roi_ds_chunks(roi, xds, valid_list)    
        da = combine_resample(ds_chunks, xy_res=xy_res)
        return da
    
    da = fn(path, roi, valid_list, xy_res)
    from utils import arr_to_plasma
    return arr_to_plasma(da.values.squeeze(), min_max=min_max, colormap=colormap), [0,0,len(da.x)/len(da.y),1]