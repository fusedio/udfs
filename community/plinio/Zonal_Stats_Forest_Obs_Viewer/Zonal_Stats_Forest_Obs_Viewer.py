@fused.udf
def udf(bbox: fused.types.TileGDF=None):
    import pandas as pd
    import geopandas as gpd

    # 1. Determine which Zonal Stats grid cells fall within the `bbox`
    s3_file_path = f"s3://fused-asset/data/zonal_stats_example/assets_with_bounds_4_4.parquet"
    gdf_bounds = fused.utils.Zonal_Stats_Forest_Obs.get_asset_dissolve(s3_file_path)
    gdf = gdf_bounds.sjoin(bbox)
    target_cells = list(set(gdf.ind.values))

    # 2. Create GeoDataFrame for every target grid cell
    arr_dfs=[]
    for each in target_cells:
        try:
            path = f"s3://fused-users/fused/plinio/assets_with_bounds_4_4_antimeridian_2jan2025_adm2_064_v2_10jan2025_v4/out_{each}.parquet"
            @fused.cache
            def read_parquet(path):
                return gpd.read_parquet(path)
            _df = read_parquet(path)
            arr_dfs.append(_df)
        except Exception as e:
            print('Error: ', e)
            continue

    # 3. Concatenate GeoDataFrames
    gdf = pd.concat(arr_dfs).reset_index()

    if len(gdf)==0: return

    # 4. Group by `shapeID` to "stitch" split municipalities
    gdf = gdf.dissolve(by='shapeID', aggfunc='sum')

    # 5. Re-joined municipalities need their mean re-calculated
    gdf['stats_mean'] = gdf['stats_sum'] / gdf['stats_count']
    return gdf.clip(bbox)
  

