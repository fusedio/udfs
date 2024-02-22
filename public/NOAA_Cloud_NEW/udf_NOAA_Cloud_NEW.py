@fused.udf()
def udf(bbox=None, bounds='-180,-10,-65,70', res=100, yr=2024, month='02', day='19', hr='13'):
    bounds=[float(i) for i in bounds.split(',')]
    from utils import bbox_to_xy_slice
    import geopandas as gpd
    import shapely
    import xarray 
    import rioxarray
    if bbox is None:
        bbox=gpd.GeoDataFrame(geometry=[shapely.box(*bounds)], crs=4326)
    # prd = 'VIS';min_max=(0,255);colormap='gray';reverse=False
    prd = 'WV';min_max=(150,210);colormap='Blues';reverse=False
    filename = f'GMGSI_{prd}/{yr}/{month}/{day}/{hr}/GLOBCOMP{prd}_nc.{yr}{month}{day}{hr}'
    path = f'https://noaa-gmgsi-pds.s3.amazonaws.com/{filename}'
    print(path)
    path = fused.utils.download(path, f'{filename}.nc')
    # ds = rioxarray.open_rasterio(path)
    ds = xarray.open_dataset(path)
    df_view = gpd.GeoDataFrame(geometry=[shapely.box(*bbox.total_bounds)])
    x, y = bbox_to_xy_slice(ds, df_view.total_bounds)
    ds = ds.sel(xc=x, yc=y)
    arr = fused.public.utils.arr_to_plasma(ds.data.values[0], min_max=min_max,colormap=colormap, reverse=reverse)
    import numpy as np
    arr = np.vstack([arr.astype(float)*100, 
                     arr.astype(float)*100,
                     arr.astype(float)*100,
                     arr.astype(float)*100,
                     arr.astype(float)*100])
    print(arr.shape)
    return arr , bbox.total_bounds
   
    # factor = ds.lat.shape[0] // res
    # if factor > 1:
    #     data = ds.data.coarsen(xc=factor, yc=factor, boundary='trim').mean()
    # else:
    #     data = ds.data
    # import pandas as pd
    # df = pd.DataFrame({'arr': [data.values.flatten()]})
    # df['shape0'] = data.values.shape[-2]
    # df['shape1'] = data.values.shape[-1]
    # df['fused_index'] = 0
    # # return df
    # df = fused.utils.geo_convert(data.to_dataframe().reset_index())
    # df = df[['geometry', 'data']]
    # df = df.sjoin(df_view)
    # df = df[df['data'] > 30].reset_index()
    # print(df) 
    # return df[['data', 'geometry']]