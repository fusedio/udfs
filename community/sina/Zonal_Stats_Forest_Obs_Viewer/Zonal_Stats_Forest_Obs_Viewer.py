@fused.udf
def udf(bounds: fused.types.Bounds= [-89.4859409525646,23.27628480644696,-73.60352698311674,32.30341711496947]):
    import pandas as pd
    import geopandas as gpd

    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    zonal_stats_forest = fused.load("https://github.com/fusedio/udfs/blob/b603e45/community/plinio/Zonal_Stats_Forest_Obs/")
    tile = common.get_tiles(bounds, clip=True)

    # 1. Determine which Zonal Stats grid cells fall within the `bounds`
    s3_file_path = f"s3://fused-asset/data/zonal_stats_example/assets_with_bounds_4_4.parquet"
    gdf_bounds = zonal_stats_forest.get_asset_dissolve(s3_file_path)
    gdf = gdf_bounds.sjoin(tile)
    target_cells = list(set(gdf.ind.values))

    # 2. Check what files actually exist in the output directory
    try:
        available_files = fused.api.list("s3://fused-asset/data/zonal_stats_example/output_table_10jan2025/")
        available_cells = set()
        for file_path in available_files:
            if file_path.endswith('.parquet') and 'out_' in file_path:
                # Extract the cell ID from filename like "out_1234.parquet"
                cell_id = int(file_path.split('out_')[-1].split('.')[0])
                available_cells.add(cell_id)
        
        # Filter target_cells to only include those that exist
        target_cells = [cell for cell in target_cells if cell in available_cells]
        print(f"Found {len(available_cells)} available files, filtering to {len(target_cells)} cells within bounds")
    except Exception as e:
        print(f"Error checking available files: {e}")
        # Continue with original target_cells if we can't check

    # 3. Create GeoDataFrame for every target grid cell that exists
    arr_dfs = []
    
    for each in target_cells:
        try:
            path = f"s3://fused-asset/data/zonal_stats_example/output_table_10jan2025/out_{each}.parquet"
            @fused.cache
            def read_parquet(path):
                return gpd.read_parquet(path)
            _df = read_parquet(path)
            arr_dfs.append(_df)
        except Exception as e:
            print(f'Error reading file for cell {each}: {e}')
            continue

    # 4. Concatenate GeoDataFrames
    if len(arr_dfs) == 0:
        print("No valid files found to process")
        return gpd.GeoDataFrame()  # Return empty GeoDataFrame instead of None
    
    gdf = pd.concat(arr_dfs).reset_index()

    # 5. Group by `shapeID` to "stitch" split municipalities
    gdf = gdf.dissolve(by='shapeID', aggfunc='sum')

    # 6. Re-joined municipalities need their mean re-calculated
    gdf['stats_mean'] = gdf['stats_sum'] / gdf['stats_count']
    
    # Ensure we have a valid result to return
    if len(gdf) > 0:
        return gdf.clip(tile)
    else:
        return gdf