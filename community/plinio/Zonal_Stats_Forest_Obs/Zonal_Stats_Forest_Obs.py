@fused.udf
def udf(
    cell_id: int = 1140,
    s3_file_path: str = "s3://fused-asset/data/zonal_stats_example/assets_with_bounds_4_4.parquet",
    geoboundary_file="adm2_064_v2",
    output_suffix="3jan2025_v2",
    use_cached_output=True
):
    import geopandas as gpd
    import numpy as np
    import pandas as pd
    import s3fs

    from utils import get_asset_dissolve, rio_clip_geom_from_url, rio_clip_geom, zonal_stats_df, get_idx_range
    # Load pinned versions of utility functions.
    common = fused.load(
    "https://github.com/fusedio/udfs/tree/ee9bec5/public/common/"
    ).utils

    # 1. Get cell bounds for the cell
    gdf_cells = get_asset_dissolve(url=s3_file_path)
    gdf_cell = gdf_cells.iloc[cell_id : cell_id + 1]

    # 2. Structure output path
    path_output = s3_file_path.split(".")[0] + f"_{geoboundary_file}_{output_suffix}/out_{cell_id}.parquet"
    print('path_output: ', path_output)
    
    # 3. If the expected output file exists, skip execution
    try:
        if use_cached_output & len(s3fs.S3FileSystem().ls(path_output)) > 0:
            print("File found. Using cached data.")
            return gpd.read_parquet(path_output)
    except FileNotFoundError:
        print(f"File not found: {path_output}. Proceeding with execution...")


    # *Handle antimeridian by shifting, if needed
    translate = False
    if gdf_cell.centroid.x.iloc[0] > 180:
        gdf_cell.geometry = gdf_cell.geometry.translate(xoff=-360)
        translate = True

    # 4. Create gdf_muni
    gdf_muni = common.table_to_tile(
        gdf_cell,
        "s3://fused-asset/data/geoboundaries/adm2_064_v2/",
        use_columns=["shapeID", "geometry"],
        clip=True,
    )

    # *Shift back if antimeridian handling was used
    if translate:
        gdf_muni.geometry = gdf_muni.geometry.translate(xoff=360)

    if len(gdf_muni) < 1:
        print("No muni data")
        return

    # return gdf_muni
    gdf_muni = gdf_muni.explode()
    gdf_muni["_idx_"] = range(len(gdf_muni))
    gdf_muni.crs = 4326
    # return gdf_muni

    # 5. Load tiff
    filename = gdf_cell[["url"]].iloc[0].values[0]
    tiff_url = f"s3://fused-asset/gfc2020/{filename}"
    geom_bbox_muni = common.geo_bbox(gdf_muni).geometry[0]
    # return geom_bbox_muni

    # 6. Get TIFF dataset
    da, _ = rio_clip_geom_from_url(geom_bbox_muni, tiff_url)

    # 7. Zonal stats
    stats_dict={
        'mean': lambda masked_value: masked_value.data[masked_value.mask].mean(),
        'sum': lambda masked_value: masked_value.data[masked_value.mask].sum(),
        'count': lambda masked_value: masked_value.data[masked_value.mask].size,
        'size': lambda masked_value: masked_value.data.size,
    }
  
    df_pre_final = zonal_stats_df(gdf_muni=gdf_muni, da=da, tiff_url=tiff_url, stats_dict=stats_dict)

    # 8. Structure final table
    df_final = pd.concat([gdf_muni.reset_index(drop=True), df_pre_final], axis=1)
    print('df_final', df_final.T)
    return df_final

    
    df_final.drop("tiff_url", axis=1, inplace=True)
    if translate:
        df_final.geometry = df_final.geometry.translate(xoff=-360)

    # 9. Recompute stats_mean
    df_final["stats_mean"] = df_final["stats_sum"] / df_final["stats_count"]

    # 10. Save
    print("Saving to", path_output)
    df_final.to_parquet(path_output)
    return df_final
    
