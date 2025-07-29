arg = {"ind": 1140}

@fused.udf
def udf(
    arg:dict =arg,
    output_dir = "s3://fused-users/fused/plinio/assets_with_bounds_4_4_antimeridian_2jan2025_adm2_064_v2_10jan2025_v4/",
    table_tif_bounds: str = "s3://fused-asset/data/zonal_stats_example/assets_with_bounds_4_4.parquet",
    table_muni_geoboundaries: str = 's3://fused-asset/data/geoboundaries/adm2_064_v2/',
    _use_cached_output: bool = False,
    save: bool=False
):
    import os
    import geopandas as gpd
    import numpy as np
    import pandas as pd
    import s3fs
    import itertools

    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    zonal_stats_forest = fused.load("https://github.com/fusedio/udfs/blob/b603e45/community/plinio/Zonal_Stats_Forest_Obs/")

    # 1. Define `cell_id` from the input dictionary
    cell_id = arg['ind']

    # 2. Create GeoDataFrame with the bounds for the specified cell id 
    gdf_cells = zonal_stats_forest.get_asset_dissolve(url=table_tif_bounds)
    cell_id = list(gdf_cells[gdf_cells['ind'] == cell_id]['ind'].values)[0]
    cell_ids_df = gdf_cells[gdf_cells['ind'] == cell_id]


    path_output = os.path.join(output_dir, f"out_{cell_id}.parquet")

    @fused.cache
    def get_s3fs_filesystem():
        return s3fs.S3FileSystem()
    s3fs_filesystem = get_s3fs_filesystem()
    
    # If the expected output file exists, skip execution
    from botocore.exceptions import NoCredentialsError
    try:
        if _use_cached_output & len(s3fs_filesystem.ls(path_output)) > 0:
            print("File found. Using cached data.")
            return gpd.read_parquet(path_output)
    except FileNotFoundError:
        print(f"File not found: {path_output}. Proceeding with execution...")
    except (NoCredentialsError, PermissionError) as e:
        print("AWS credentials not found. Please configure your AWS credentials and try again.", e)

    # Create gdf of the target cell
    gdf_cell = gdf_cells[gdf_cells['ind'] == cell_id]

    # *Handle antimeridian by shifting, if needed
    translate = False
    if gdf_cell.centroid.x.iloc[0] > 180:
        gdf_cell.geometry = gdf_cell.geometry.translate(xoff=-360)
        translate = True

    # 3. Create gdf_muni
    gdf_muni = common.table_to_tile(
        gdf_cell,
        table_muni_geoboundaries,
        use_columns=["shapeID", "geometry"],
        clip=True,
    )

    # *Shift back if antimeridian handling was used
    if translate:
        gdf_muni.geometry = gdf_muni.geometry.translate(xoff=360)

    if len(gdf_muni) < 1:
        print("No muni data")
        return

    gdf_muni = gdf_muni.explode()
    gdf_muni.crs = 4326

    # 4. Load tiff
    filename = gdf_cell[["url"]].iloc[0].values[0]
    tiff_url = f"s3://fused-asset/gfc2020/{filename}"
    geom_bounds_muni = common.geo_bbox(gdf_muni).geometry[0]

    # 5. Get TIFF dataset
    da, _ = zonal_stats_forest.rio_clip_geom_from_url(geom_bounds_muni, tiff_url)
    
    # 6. Zonal stats
    stats_dict={
        'mean': lambda masked_value: masked_value.data[masked_value.mask].mean(),
        'sum': lambda masked_value: masked_value.data[masked_value.mask].sum(),
        'count': lambda masked_value: masked_value.data[masked_value.mask].size,
        'size': lambda masked_value: masked_value.data.size,
    }
  
    df_pre_final = zonal_stats_forest.zonal_stats_df(gdf_muni=gdf_muni, da=da, tiff_url=tiff_url, stats_dict=stats_dict)

    # 7. Structure final table
    df_final = pd.concat([gdf_muni.reset_index(drop=True), df_pre_final], axis=1)
    df_final.drop("tiff_url", axis=1, inplace=True)

    # *Shift back if antimeridian handling was used
    if translate:
        df_final.geometry = df_final.geometry.translate(xoff=-360)

    # 8. Recompute stats_mean
    df_final["stats_mean"] = df_final["stats_sum"] / df_final["stats_count"]

    # 9. Save
    if save==True:
        path_output = os.path.join(output_dir, f"out_{cell_id}.parquet")
        print("Saving to", path_output)
        df_final.to_parquet(path_output)


    return df_final


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





    
