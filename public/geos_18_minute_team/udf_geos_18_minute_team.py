#TODO make sure do not cache the future
# https://noaa-goes18.s3.amazonaws.com/index.html#ABI-L1b-RadC/2024/001/00/
@fused.udf
def udf(i=10, product_name='ABI-L2-CMIPF', bucket_name='noaa-goes18', datestr='2024-01-31', offset='0', band=8, x_res=4000, y_res=4000): 
    i=int(i);band=int(band);x_res=int(x_res);y_res=int(y_res);
    chunk_size = (1200, 1200)
    min_max=(1800,2800)
    import geopandas as gpd
    from shapely.geometry import Polygon
    import json
    roi_wkt = '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {"location": "OR_ABI-L2-CMIPF-M6C09_G18_s20240290000212_e20240290009526_c20240290009593.nc_colored.tif"}, "geometry": {"type": "Polygon", "coordinates": [[[-4000000.0, 2500000.0], [3000000.0, 2500000.0], [3000000.0, -1500000.0], [-4000000.0, -1500000.0], [-4000000.0, 2500000.0]]]}}]}'
    crs = 'PROJCRS["WGS84 / Lambert_Conformal_Conic_2SP",BASEGEOGCRS["WGS84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1,ID["EPSG",9001]]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]]],CONVERSION["unnamed",METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],PARAMETER["Latitude of false origin",33,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]],PARAMETER["Longitude of false origin",-125,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Latitude of 1st standard parallel",21,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",45,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Easting at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8827]]],CS[Cartesian,2],AXIS["easting",east,ORDER[1],LENGTHUNIT["Meter",1]],AXIS["northing",north,ORDER[2],LENGTHUNIT["Meter",1]]]'
    json_data = json.loads(roi_wkt)
    roi = gpd.GeoDataFrame(geometry=[Polygon(feature["geometry"]["coordinates"][0]) for feature in json_data["features"]]
                               ,crs=crs)
    @fused.cache
    def cache_valid_list(roi, product_name, bucket_name, chunk_size): 
        from utils import file_of_day, chunkify, roi_ds_chunks
        import rioxarray
        file_path = file_of_day(0, bucket_name, product_name, band, '2024-01-01') 
        print(file_path)
        url=f'https://{bucket_name}.s3.amazonaws.com/{file_path}'
        xds = rioxarray.open_rasterio(url)
        xy_size = (len(xds.x),len(xds.y))
        slice_list = chunkify(chunk_size=chunk_size, xy_size=xy_size, pad=10)
        valid_list = roi_ds_chunks(roi, xds, slice_list, return_data=False) 
        return valid_list  
    @fused.cache
    def fn(roi, i, product_name, datestr, offset, band, x_res, y_res):
        from utils import file_of_day, chunkify, roi_ds_chunks, combine_resample
        #load data  
        import rioxarray
        file_path = file_of_day(i, bucket_name, product_name, band, datestr,offset) 
        filename = file_path.split('/')[-1]
        url = f'https://{bucket_name}.s3.amazonaws.com/{file_path}'
        path = fused.utils.download(url,'geos18/'+filename)
        xds = rioxarray.open_rasterio(path)
        #chunk & reproject data
        xy_size = (len(xds.x),len(xds.y))
        slice_list = chunkify(chunk_size=chunk_size, xy_size=xy_size, pad=10)
        # valid_list = cache_valid_list(roi, product_name, bucket_name, chunk_size)
        valid_list = [[slice(1190, 2390, None), slice(0, 1200, None)], [slice(2380, 3580, None), slice(0, 1200, None)], [slice(3570, 4770, None), slice(0, 1200, None)], [slice(1190, 2390, None), slice(1190, 2390, None)], [slice(2380, 3580, None), slice(1190, 2390, None)], [slice(3570, 4770, None), slice(1190, 2390, None)]]
        ds_chunks = roi_ds_chunks(roi, xds, slice_list, valid_list)    

        #combine data
        da = combine_resample(ds_chunks, xy_res=(x_res,y_res))
        print(1)
        return da
    
    da = fn(roi, i, product_name, datestr=datestr, offset=offset, band=band, x_res=x_res, y_res=y_res)
    from utils import arr_to_plasma
    return arr_to_plasma(da.values.squeeze(), min_max=min_max, colormap=None), [0,0,len(da.x)/len(da.y),1]