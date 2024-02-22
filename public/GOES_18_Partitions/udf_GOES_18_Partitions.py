#TODO make sure do not cache the future
# https://noaa-goes18.s3.amazonaws.com/index.html#ABI-L1b-RadC/2024/001/00/
@fused.udf
def udf(url='https://noaa-goes18.s3.amazonaws.com/ABI-L2-CMIPF/2024/031/01/OR_ABI-L2-CMIPF-M6C08_G18_s20240310140216_e20240310149524_c20240310149597.nc',
        roi_wkt = '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[-4100000.0, 2400000.0], [3200000.0, 2400000.0], [3200000.0, -1500000.0], [-4100000.0, -1500000.0], [-4100000.0, 2400000.0]]]}}]}',
        crs = 'PROJCRS["WGS84 / Lambert_Conformal_Conic_2SP",BASEGEOGCRS["WGS84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1,ID["EPSG",9001]]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]]],CONVERSION["unnamed",METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],PARAMETER["Latitude of false origin",33,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]],PARAMETER["Longitude of false origin",-125,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Latitude of 1st standard parallel",21,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",45,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Easting at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8827]]],CS[Cartesian,2],AXIS["easting",east,ORDER[1],LENGTHUNIT["Meter",1]],AXIS["northing",north,ORDER[2],LENGTHUNIT["Meter",1]]]',
        chunk_len=1200): 
    chunk_size = (int(chunk_len), int(chunk_len))
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Polygon 
    import json
    json_data = json.loads(roi_wkt)
    roi = gpd.GeoDataFrame(geometry=[Polygon(feature["geometry"]["coordinates"][0]) for feature in json_data["features"]]
                               ,crs=crs)
    @fused.cache
    def cache_valid_list(roi, url, chunk_size): 
        from utils import chunkify, roi_ds_chunks
        import rioxarray
        xds = rioxarray.open_rasterio(url)
        xy_size = (len(xds.x),len(xds.y))
        slice_list = chunkify(chunk_size=chunk_size, xy_size=xy_size, pad=10)
        valid_list = roi_ds_chunks(roi, xds, slice_list, return_data=False) 
        return valid_list  
    chunks=cache_valid_list(roi, url, chunk_size)  
    df = pd.DataFrame([[i[0].start, i[0].stop, i[1].start, i[1].stop] for i in chunks], columns =['x_start','x_stop', 'y_start','y_stop'])
    return df