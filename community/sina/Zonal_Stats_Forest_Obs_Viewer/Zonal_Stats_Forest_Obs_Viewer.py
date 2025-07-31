@fused.udf
def udf(bounds: fused.types.Bounds= [-89.4859409525646,23.27628480644696,-73.60352698311674,32.30341711496947]):
    import pandas as pd
    import geopandas as gpd

    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    zonal_stats_forest = fused.load("https://github.com/fusedio/udfs/blob/b603e45/community/plinio/Zonal_Stats_Forest_Obs/")
    tile = common.get_tiles(bounds, clip=True)

    # 1. Determine which Zonal Stats grid cells fall within the `bounds`
    s3_file_path = f"s3://fused-asset/data/zonal_stats_example/assets_with_bounds_4_4.parquet"
    gdf_bounds = zonal_stats_forest.get_asset_dissolve(s3_file_path)
    gdf = gdf_bounds.sjoin(tile)
    target_cells = list(set(gdf.ind.values))

    # 2. Create GeoDataFrame for every target grid cell
    arr_dfs=[]
    
    for each in target_cells:
        try:
            path = f"s3://fused-asset/data/zonal_stats_example/output_table_10jan2025/out_{each}.parquet"
            @fused.cache
            def read_parquet(path):
                return gpd.read_parquet(path)
            _df = read_parquet(path)
            arr_dfs.append(_df)
        except Exception as e:
            print('Error: ', e)
            continue

    # 3. Concatenate GeoDataFrames
    if len(arr_dfs)==0: return
    gdf = pd.concat(arr_dfs).reset_index()

    # 4. Group by `shapeID` to "stitch" split municipalities
    gdf = gdf.dissolve(by='shapeID', aggfunc='sum')

    # 5. Re-joined municipalities need their mean re-calculated
    gdf['stats_mean'] = gdf['stats_sum'] / gdf['stats_count']
    return gdf.clip(tile)
  

