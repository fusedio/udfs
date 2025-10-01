@fused.udf
def udf(
    bounds: fused.types.Bounds = [-180, -90, 180, 90],
    layer:str = 'cnc_dust',
    min_max = (0, 3000),
    year:int = 2025,
    month:int = 1,
    day:int = 27,
    step:int = 80
):

    # hello
    import pandas as pd
    import geopandas as gpd
    import numpy as np
    import zarr
    import xarray as xr
    import palettable
    
    from datetime import datetime

    # Load utils
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")

    # Open zarr dataset
    ds = xr.open_zarr("s3://us-west-2.opendata.source.coop/bkr/silam-dust/silam_global_dust_v3.zarr/")
    print(ds)
    print("-"*75)
    
    # Currently zarr data exists from 11-14-2024 - 27-01-2025
    init_time_dt = datetime(year, month, day)
    print("init_time: ", init_time_dt)

    # Get bounds
    minx, miny, maxx, maxy = bounds

    ds_slice = ds.sel(
        init_time=init_time_dt,
        latitude=slice(float(miny), float(maxy)),
        longitude=slice(float(minx), float(maxx)),
    ).isel(step=step)
    
    # Process data layer
    da = ds_slice[layer] * ds_slice["air_dens"] * 1e9  # kg/kg * kg/m³ → µg/m³
    print(da)

    # Reverse latitude ordering 
    da = da.sortby('latitude', ascending=False)

    # Create cmap and bins for visualization
    bins = [0, 3, 6, 15, 30, 60, 150, 300, 600, 1500, 3000, np.inf]
    colors = [
        "#0000FF", "#008080", "#66CDAA", "#006400", "#90EE90",
        "#FFFF00", "#FFA500", "#FF8C00", "#FF0000", "#C71585", "#C71585"
    ]
    
    # Reproject data
    arr = da.rio.set_crs("EPSG:4326")
    data_array = arr.values.squeeze()
    
    # Mask and visualize
    masked_data = np.nan_to_num(data_array, nan=0)
    mask = data_array >= bins[1]
    
    viz = common_utils.visualize(
        data=masked_data,
        mask=mask,
        min=min_max[0],
        max=min_max[1],
        colormap=colors,
        colorbins=bins
        
    )
    return viz
