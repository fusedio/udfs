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

    # 1. Define `cell_id` from the input dictionary
    cell_id = arg['ind']

    # 2. Create GeoDataFrame with the bounds for the specified cell id 
    gdf_cells = fused.utils.Zonal_Stats_Forest_Obs.get_asset_dissolve(url=table_tif_bounds)
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
    gdf_muni = fused.utils.common.table_to_tile(
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
    geom_bbox_muni = fused.utils.common.geo_bbox(gdf_muni).geometry[0]

    # 5. Get TIFF dataset
    da, _ = fused.utils.Zonal_Stats_Forest_Obs.rio_clip_geom_from_url(geom_bbox_muni, tiff_url)
    
    # 6. Zonal stats
    stats_dict={
        'mean': lambda masked_value: masked_value.data[masked_value.mask].mean(),
        'sum': lambda masked_value: masked_value.data[masked_value.mask].sum(),
        'count': lambda masked_value: masked_value.data[masked_value.mask].size,
        'size': lambda masked_value: masked_value.data.size,
    }
  
    df_pre_final = fused.utils.Zonal_Stats_Forest_Obs.zonal_stats_df(gdf_muni=gdf_muni, da=da, tiff_url=tiff_url, stats_dict=stats_dict)

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


    
