@fused.udf
def udf(
    bounds: fused.types.Bounds = [44.935,-10.364,106.628,48.359],
    layer: str = "tas",
    time: int = 2,
    target_shape: list = [512, 512],
    valid_min: int = 250,
    valid_max: int = 300,
):
    import numpy as np
    import xarray as xr

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/2f41ae1/public/common/").utils
    tile = common_utils.get_tiles(bounds, clip=True)


    ds = xr.open_zarr("gs://fused_public/zarr/wri_cmip6_median_ssp585.zarr")

    if tile["z"].iloc[0] < 1:
        print("z less than 1")
        return
    utils = fused.load("https://github.com/fusedio/udfs/tree/cbc5482/public/common/").utils

    minx, miny, maxx, maxy = bounds
    variable_names = list(ds.data_vars)

    # Printing the Time Range of the Dataset
    if "time" in ds.coords:
        time_values = ds["time"].values
        num_time_steps = len(time_values)
        first_time = time_values[0]
        last_time = time_values[-1]
    else:
        print("No time dimension found in the dataset.")

    # Selecting subset and tile for the current tile
    buffer = ds.lon[1] - ds.lon[0]
    ds = ds.sel(time=2080)
    ds_buffer = ds.sel(lat=slice(miny - buffer, maxy + buffer), lon=slice(minx - buffer, maxx + buffer))
    da = utils.da_fit_to_resolution(ds_buffer[layer], target_shape)
    da = da.sel(lat=slice(miny, maxy), lon=slice(minx, maxx))
    da = da.rename({"lat": "y", "lon": "x"})
    # Reprojecting to Web Mercator for visualization
    arr = da.rio.set_crs("EPSG:4326")
    arr_reprojected = arr.rio.reproject("EPSG:3857")
    data_array = arr_reprojected.values.squeeze()

    # Compute min and max values, excluding NaN
    valid_min = valid_min or math.floor(np.nanmin(data_array))
    valid_max = valid_max or math.ceil(np.nanmax(data_array))
    print(valid_min)
    print(valid_max)

    # Masking the NaN values and replcing with values outside min max
    masked_data = np.nan_to_num(data_array, nan=valid_min - 1)
    # Using the minmax for color mapping
    arr = utils.arr_to_plasma(
        masked_data, min_max=(valid_min, valid_max), colormap="RdYlBu"
    )
    return arr
