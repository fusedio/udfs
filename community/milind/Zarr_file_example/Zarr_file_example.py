@fused.udf
def udf(
    bbox: fused.types.Bbox=None, 
    layer: str='lccs_class_7'
):
    import geopandas as gpd
    import shapely
    import xarray as xr 
    import rioxarray
    import numpy as np
    import os
    utils = fused.load("https://github.com/fusedio/udfs/tree/f928ee1/public/common/").utils
    
    ds = xr.open_zarr('s3://fused-users/fused/plinio/seasfire_v3/')

    print(bbox)
    print(ds)
    minx, miny, maxx, maxy = bbox.bounds
    variable_names = list(ds.data_vars)
    #printing all the variable names in the dataset
    print("Variable names in the dataset:")
    for name in variable_names:
        print(name)

    #time parameter, can be changed to view different results
    time = 1
    
    ds = ds.sel(
        latitude=slice(maxy, miny), 
        longitude=slice(minx, maxx)
    ).isel(time=time)

    data_array = ds[layer].values.squeeze()
    print(data_array)
    
    # Compute min and max values, excluding NaN 
    valid_min = np.nanmin(data_array)
    valid_max = np.nanmax(data_array)
    
    # masking the NaN values and replcing with values outside min max
    masked_data = np.nan_to_num(data_array, nan=valid_min - 1)

    #using the minmax for color mapping
    arr = utils.arr_to_plasma(
        masked_data, 
        min_max=(valid_min, valid_max)
    )
    return arr
