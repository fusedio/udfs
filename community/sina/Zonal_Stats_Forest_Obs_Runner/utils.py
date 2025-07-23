@fused.cache
def get_idx_range(target_url, s3_file_path):
    target_urls = [target_url]
    df = get_asset_dissolve(s3_file_path)
    # Subselect for geometries in target tiff files
    df = df[df['url'].isin(target_urls)]
    # The indices of the cells that belong to the target files
    idx_range = df.index.values    
    return idx_range

def get_asset_dissolve(
    url=f"s3://fused-users/fused/plinio/assets_with_bounds_4_4_antimeridian.parquet",
    order=0,
):
    import geopandas as gpd

    gdf = gpd.read_parquet(url)
    gdf["url"] = gdf["url"].map(lambda x: x[order])
    gdf["fused_sub_id"] = gdf["fused_sub_id"].map(lambda x: x[order])
    gdf = gdf.dissolve(["fused_sub_id", "url"])
    gdf.reset_index(inplace=True)
    gdf["ind"] = range(len(gdf))
    del gdf["sub_bounds"]
    return gdf


def rio_geom_to_xy_slice(geom, shape, transform):
    import shapely
    import rasterio
    local_bounds = shapely.bounds(geom)
    if transform[4] < 0:  # if pixel_height is negative
        original_window = rasterio.windows.from_bounds(
            *local_bounds, transform=transform
        )
        gridded_window = rasterio.windows.round_window_to_full_blocks(
            original_window, [(1, 1)]
        )
        y_slice, x_slice = gridded_window.toslices()
        return x_slice, y_slice
    else:  # if pixel_height is not negative
        original_window = rasterio.windows.from_bounds(
            *local_bounds,
            transform=Affine(
                transform[0],
                transform[1],
                transform[2],
                transform[3],
                -transform[4],
                -transform[5],
            ),
        )
        gridded_window = rasterio.windows.round_window_to_full_blocks(
            original_window, [(1, 1)]
        )
        y_slice, x_slice = gridded_window.toslices()
        y_slice = slice(shape[0] - y_slice.stop, shape[0] - y_slice.start + 0)
        return x_slice, y_slice


def _expand_slice(s, expand_size=1):
    return slice(max(s.start - expand_size, 0), s.stop + expand_size, s.step)


def rio_clip_geom(geom, da):
    """
    Clips a geographic region from a given data array based on the provided geometry.

    Parameters:
    geom (dict): A dictionary representing the geometry of the region to clip.
    da (xarray.DataArray): The data array to clip.

    Returns:
    tuple: A tuple containing the clipped data array and its values.

    """
    x_slice, y_slice = rio_geom_to_xy_slice(
        geom, shape=da.shape[1:], transform=da.rio.transform()
    )
    x_slice = _expand_slice(x_slice, 1)
    y_slice = _expand_slice(y_slice, 1)
    da = da.isel(x=x_slice, y=y_slice)

    return da, da.values


def rio_clip_geom_from_url(geom, url):
    """
    Clips a geographic region from a raster dataset accessed via a URL.

    Parameters:
    geom (dict): A dictionary representing the geometry of the region to clip.
    url (str): The URL of the raster dataset.

    Returns:
    tuple: A tuple containing the clipped data array and its values.
           Returns (-1, -1) if an exception occurs.

    """
    import xarray
    try:
        path = fused.download(url, url + ".tiff")
        ds = xarray.open_dataset(path, engine="rasterio", lock=False)
        da = ds.band_data
        return rio_clip_geom(geom, da)

    except Exception as e:
        print(f"Error: {e}")
        return -1, -1

def zonal_stats_df(gdf_muni, da, tiff_url, stats_dict):
    from rasterio.features import geometry_mask
    import numpy as np
    import pandas as pd
    stats_list = []
    for i in range(len(gdf_muni)):
        if isinstance(da, int):
            stats_list.append([tiff_url, i]+ [[-1]*len(stats_dict)]) # Tagged error state: image loading broke
            continue

        geom = gdf_muni.geometry.iloc[i]
        try:
            da_geom, value_geom = rio_clip_geom(geom, da)

            if da_geom.shape[-1] * da_geom.shape[-2] == 0:
                stats_list.append([tiff_url, i]+ [[0]*len(stats_dict)]) # Tagged error state: da_geom has a zero dimension
                continue

            geom_mask = ~geometry_mask(
                [geom],
                transform=da_geom.rio.transform(),
                invert=True,
                out_shape=da_geom.shape[-2:],
            )

            # Calculate stats for mask area
            if geom_mask.any():
                masked_value = np.ma.MaskedArray(data=value_geom, mask=[~geom_mask])
                stats_calculations = [calc(masked_value) for calc in stats_dict.values()]
                thearr = [tiff_url,i]+stats_calculations
                stats_list.append(thearr)
            else:
                stats_list.append([tiff_url, i]+ [[-2]*len(stats_dict)]) # Tagged error state: there's no mask area
        except Exception as e:
            print("Error: ", e)
            return

    # 6. Structure final df
    df_pre_final = pd.DataFrame(
        stats_list,
        columns=[
            "tiff_url",
            "i",
            "stats_mean",
            "stats_sum",
            "stats_count",
            "stats_size",
            
        ],
    )
    return df_pre_final


