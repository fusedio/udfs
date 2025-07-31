@fused.udf
def udf():
    import geopandas as gpd
    import shapely
    import pandas as pd
    from shapely import wkt
    import shapely
    import h3
    import json
    import duckdb
    from shapely.geometry import Polygon
    import numpy as np
    common = fused.load("https://github.com/fusedio/udfs/tree/6e8abb9/public/common/")
    con = duckdb.connect()
    h3_size=8
    
    # 1. Load data from output directories
    table = 's3://fused-asset/misc/samroy/overture_output_h3_v3/dataset=*/geoid=*/*.parquet'
    @fused.cache
    def load(table):
        v=1
        df = con.sql(f"""
            SELECT * FROM read_parquet('{table}',hive_partitioning = true)
            WHERE geoid='06'
            """).df()
        return df
    
    gdf1 = load(table)

    @fused.cache
    def run(gdf):
        df_overture = gdf[gdf['dataset'] == 'overture']
        df_oakridge = gdf[gdf['dataset'] == 'oakridge']
    
        # 2. Introduce empty H3
        geoid = '06'
        gdf_state = gpd.read_parquet('s3://fused-asset/data/tiger/state/tl_rd22_us_state 1pct.parquet').to_crs("EPSG:4326")
        gdf_state = gdf_state[gdf_state['GEOID'] == geoid]
    
        row = gdf_state.iloc[0]
        gdf_state['hex'] = gdf_state['geometry'].apply(lambda x: h3.geo_to_cells(x, res=h3_size))
        gdf_state = gdf_state.explode('hex')
        gdf_state['avg_area'] = np.nan
        gdf_state['avg_perimeter'] = np.nan
        gdf_state['cnt'] = np.nan
        gdf_state['avg_height'] = np.nan
        out = gdf_state[['hex', 'avg_area', 'avg_perimeter', 'cnt', 'avg_height']]
        df_overture = df_overture.merge(out,on="hex", how="outer", suffixes=("", "_out"))
        df_oakridge = df_oakridge.merge(out,on="hex", how="outer", suffixes=("", "_out"))
        for col in ['avg_area', 'avg_perimeter', 'cnt', 'avg_height']:
            df_overture[col] = df_overture[col].combine_first(df_overture[f'{col}_out']).fillna(-1)
            df_overture.drop(columns=[f'{col}_out'], inplace=True)
            df_oakridge[col] = df_oakridge[col].combine_first(df_oakridge[f'{col}_out']).fillna(-1)
            df_oakridge.drop(columns=[f'{col}_out'], inplace=True)
        print(set(df_oakridge.cnt.values))
        # return df_oakridge[['hex', 'cnt']]
        # return df_overture[['hex', 'cnt']]
    
        df_combined = df_overture.merge(df_oakridge, on="hex", how="outer", suffixes=("_over", "_oakr"))
        print(df_combined.T)
        
        return df_combined
    gdf = run(gdf1)#.sample(1999)
  
    # 3. Introduce geometry
    # gdf['geometry'] = gdf['hex'].apply(lambda x: Polygon([(lon, lat) for lat, lon in h3.cell_to_boundary(x)]))
    # gdf = gpd.GeoDataFrame(gdf, geometry='geometry')
    df_overture = gdf1[gdf1['dataset'] == 'overture']
    df_oakridge = gdf1[gdf1['dataset'] == 'oakridge']
    print(df_overture.T)
    return df_overture[['hex', 'cnt']]#.head(100)
    # return gdf[['geometry', 'cnt_oakr']]
    outt = gdf#[['hex', 'cnt_oakr']]
    print(len(outt))
    print(outt.head().T)
    return outt
    return gdf[['hex', 'cnt']]