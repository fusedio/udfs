@fused.udf
def udf(bbox: fused.types.Bbox = None, layer: str = "ndvi", time: int = 2):
    import math
    import os

    import geopandas as gpd
    import numpy as np
    import rioxarray
    import shapely
    import xarray as xr

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils

    ds = xr.open_zarr("s3://fused-asset/data/seasfire_v3/")

    print(ds)
    minx, miny, maxx, maxy = bbox.bounds
    variable_names = list(ds.data_vars)

    # Printing all the variable names in the dataset
    print("Variable names in the dataset:")
    for name in variable_names:
        print(name)

    # Printing the Time Range of the Dataset
    if "time" in ds.coords:
        time_values = ds["time"].values
        num_time_steps = len(time_values)
        first_time = time_values[0]
        last_time = time_values[-1]
        print(f"First time step: {first_time} (index 0)")
        print(f"Last time step: {last_time} (index {num_time_steps - 1})")
        print(f"Number of time steps: {num_time_steps}")
    else:
        print("No time dimension found in the dataset.")

    ds = ds.sel(latitude=slice(maxy, miny), longitude=slice(minx, maxx)).isel(time=time)

    data_array = ds[layer].values.squeeze()
    print(data_array)

    # Compute min and max values, excluding NaN
    valid_min = math.floor(np.nanmin(data_array))
    valid_max = math.ceil(np.nanmax(data_array))

    # Masking the NaN values and replcing with values outside min max
    masked_data = np.nan_to_num(data_array, nan=valid_min - 1)

    # Using the minmax for color mapping
    arr = utils.arr_to_plasma(masked_data, min_max=(valid_min, valid_max))
    return arr
