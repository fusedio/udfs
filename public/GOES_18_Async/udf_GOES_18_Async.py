@fused.udf
def udf(roi_wkt = '{"type": "FeatureCollection", "features": [{"id": "0", "type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[[-4100000.0, 2400000.0], [3200000.0, 2400000.0], [3200000.0, -1500000.0], [-4100000.0, -1500000.0], [-4100000.0, 2400000.0]]]}}]}',
        crs = 'PROJCRS["WGS84 / Lambert_Conformal_Conic_2SP",BASEGEOGCRS["WGS84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1,ID["EPSG",9001]]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]]],CONVERSION["unnamed",METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],PARAMETER["Latitude of false origin",33,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]],PARAMETER["Longitude of false origin",-125,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Latitude of 1st standard parallel",21,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",45,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Easting at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",0,LENGTHUNIT["Meter",1],ID["EPSG",8827]]],CS[Cartesian,2],AXIS["easting",east,ORDER[1],LENGTHUNIT["Meter",1]],AXIS["northing",north,ORDER[2],LENGTHUNIT["Meter",1]]]', 
        partition_str = '{"x_start":{"0":1190,"1":2380,"2":3570,"3":1190,"4":2380,"5":3570},"x_stop":{"0":2390,"1":3580,"2":4770,"3":2390,"4":3580,"5":4770},"y_start":{"0":0,"1":0,"2":0,"3":1190,"4":1190,"5":1190},"y_stop":{"0":1200,"1":1200,"2":1200,"3":2390,"4":2390,"5":2390},"fused_index":{"0":0,"1":1,"2":2,"3":3,"4":4,"5":5}}',
        datestr='2024-02-05', start_i=0, end_i=6 , band=8, product_name='ABI-L2-CMIPF', runner_udf_name='GOES_18_Runner', runner_email='sina@fused.io', min_pixel_value=1500, max_pixel_value=2800, colormap=''):
    start_i=int(start_i); end_i=int(end_i); band=int(band);
    from utils import run_batch
    import pandas as pd
    import numpy as np
    frames = run_batch(datestr, start_i, end_i, partition_str, roi_wkt, crs, udf_name=runner_udf_name, email=runner_email, colormap=colormap, min_pixel_value=min_pixel_value, max_pixel_value=max_pixel_value, band=band, product_name=product_name)
    frames_raw=[i.image.values[:, 1:-1, 1:-1] for i in frames if i is not None]
    print(len(frames_raw))
    frames_all = np.stack(frames_raw) 
    df = pd.DataFrame({'arr': [frames_all.flatten()]})
    df['shape'] = [frames_all.shape] 
    # print(df)
    # print(frames_all.shape)
    return df
## how to use: 
# df = fused.utils.run_file("sina@fused.io", "geos_18_async", )
# arr = df['arr'].apply(lambda x: x.reshape(df['shape'][0]))[0]