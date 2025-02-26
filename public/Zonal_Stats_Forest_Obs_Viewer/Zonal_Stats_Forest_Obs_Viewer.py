@fused.udf
def udf(bbox: fused.types.Tile=None):
    import pandas as pd
    import geopandas as gpd

    zonal_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/community/plinio/Zonal_Stats_Forest_Obs/").utils

    # 1. Determine which Zonal Stats grid cells fall within the `bbox`
    s3_file_path = f"s3://fused-asset/data/zonal_stats_example/assets_with_bounds_4_4.parquet"
    gdf_bounds = zonal_utils.get_asset_dissolve(s3_file_path)
    gdf = gdf_bounds.sjoin(bbox)
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
    return gdf.clip(bbox)
  

