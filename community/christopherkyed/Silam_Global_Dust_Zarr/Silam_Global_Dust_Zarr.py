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
    """UDF to visualize a dust concentration layer from the SILAM dataset."""
    
    import numpy as np
    import zarr
    
    from datetime import datetime

    # Load utils
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/abf9c87/public/common/")

    store = zarr.storage.FsspecStore.from_url(
       's3://us-west-2.opendata.source.coop/bkr/silam-dust/silam_global_dust_v3.zarr',
       read_only=True,
       storage_options={'anon': True}
    )
    # Open Zarr group and print dataset information
    z = zarr.open_group(store=store, mode='r')
    
    # Get coordinate arrays
    lat = z["latitude"][:]      # 1D latitudes
    lon = z["longitude"][:]     # 1D longitudes
    
    # Define your base datetime
    init_times_raw = z["init_time"][:]  # datetime64 array
    base_date = np.datetime64("2024-11-14")
    init_times = base_date + init_times_raw.astype("timedelta64[D]")

    # time steps
    steps = z["step"][:]

    
    # Currently zarr data exists from 11-14-2024 - 27-01-2025
    init_time_dt = datetime(year, month, day)
    print("init_time: ", init_time_dt)

    print(bounds)
    # Get bounds
    minx, miny, maxx, maxy = bounds

    # 1. Find matching init_time index
    init_idx = np.where(init_times == np.datetime64(init_time_dt))[0][0]
    
    # 2. Get index ranges for lat/lon slices
    lat_mask = (lat >= float(miny)) & (lat <= float(maxy))
    lon_mask = (lon >= float(minx)) & (lon <= float(maxx))
    
    lat_idx = np.where(lat_mask)[0]
    lon_idx = np.where(lon_mask)[0]
    
    # 3. Select data from the zarr array directly
    # Extract variable slices separately from zarr
    var_arr = z[layer][init_idx, step, lat_idx.min():lat_idx.max()+1, lon_idx.min():lon_idx.max()+1]
    air_dens_arr = z["air_dens"][init_idx, step, lat_idx.min():lat_idx.max()+1, lon_idx.min():lon_idx.max()+1]
    
    # Compute concentration
    da = var_arr * air_dens_arr * 1e9  # kg/kg * kg/m³ → µg/m³

    print(da)

    # Create cmap and bins for visualization
    bins = [0, 3, 6, 15, 30, 60, 150, 300, 600, 1500, 3000, np.inf]
    colors = [
        "#0000FF", "#008080", "#66CDAA", "#006400", "#90EE90",
        "#FFFF00", "#FFA500", "#FF8C00", "#FF0000", "#C71585", "#C71585"
    ]


    data_array = np.flipud(da)   # flip horizontally

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
