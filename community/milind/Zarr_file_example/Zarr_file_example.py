@fused.udf
def udf(bounds: fused.types.Bounds = None, layer: str = "ndvi", time: int = 2, target_shape: list = [512,512]):
    import geopandas as gpd
    import numpy as np
    import rioxarray
    import shapely
    import xarray as xr
    

    # Load utils
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")

    # Open dataset
    ds = xr.open_zarr("s3://fused-asset/data/seasfire_v3/")
    print(ds)
    
    # Get bounds
    minx, miny, maxx, maxy = bounds
     
    # Print dataset info
    variable_names = list(ds.data_vars)
    print("Variable names in the dataset:")
    for name in variable_names:
        print(name)
        
    # Print time info
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

    # Calculate buffer size (ensure float)
    buffer = float(ds.longitude[1].values) - float(ds.longitude[0].values)
    
    # Select data with buffer
    ds_buffer = ds.sel(
        latitude=slice(float(maxy+buffer), float(miny-buffer)), 
        longitude=slice(float(minx-buffer), float(maxx+buffer))
    ).isel(time=time)
    
    # Process data
    da = common.da_fit_to_resolution(ds_buffer[layer], target_shape)
    da = da.sel(latitude=slice(miny, maxy), longitude=slice(minx, maxx))
    
    # Reproject to Web Mercator for visualization
    arr = da.rio.set_crs("EPSG:4326")
    arr_reprojected = arr.rio.reproject("EPSG:3857")
    data_array = arr_reprojected.values.squeeze()
    print(data_array.shape)
    
    # Mask and normalize
    masked_data = np.nan_to_num(data_array, nan=-2)
    arr = common.arr_to_plasma(masked_data, min_max=(-1, 1))
    
    return arr