@fused.udf
def udf():
    import geopandas as gpd
    from shapely import wkt
    from shapely.wkt import loads
    import pandas as pd
    import duckdb
    from shapely.wkt import loads
    conn = duckdb.connect()
    import numpy as np
    import matplotlib.pyplot as plt
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_squared_error

    @fused.cache
    def load_sif_data():
        conn.sql("INSTALL spatial; LOAD spatial; INSTALL httpfs; LOAD httpfs;")
        table = 's3://fused-asset/misc/plinio/sif_output/county=*/year=*/month=*/*.parquet'
        df = conn.sql(f"""SELECT * FROM read_parquet('{table}',hive_partitioning = true)""").df()
        df['geometry'] = df['geometry'].apply(loads)
        gdf_sif = gpd.GeoDataFrame(df)
        return gdf_sif

    gdf_sif = load_sif_data()

    # df_metric = conn.sql(f"""SELECT year, count(*) FROM read_parquet('{table}',hive_partitioning = true) GROUP BY year ORDER BY year LIMIT 10""").df()
    # print(df_metric)


    @fused.cache
    def load_target_data():
        # Bring in target data
        years = ["2015", "2016", "2017", "2018", "2019", "2020"]
        dfs = []
        # path = 's3://soldatanasasifglobalifoco2modis1863/USDA/data/Actual_yields/USDA_crop_yields_2018.csv'
        # Fetch for each year
        for year in years:
            path = f's3://soldatanasasifglobalifoco2modis1863/USDA/data/Actual_yields/USDA_crop_yields_{year}.csv'
            df = pd.read_csv(path)
            
            # Select only the relevant columns
            cols = [
                'Value',  # Bushels / acre
                'Year',
                'County ANSI',
            ]
            # df = df[cols]
            
            dfs.append(df)
        
        # Concatenate all DataFrames into a single DataFrame
        df_actuals = pd.concat(dfs, ignore_index=True)
        print(df_actuals)
        return df_actuals

    df_actuals = load_target_data()
    df_actuals['GEOID'] = df_actuals['State ANSI'].astype(str).str.zfill(2) + df_actuals['County ANSI'].apply(lambda x: str(int(x)).zfill(3) if not pd.isna(x) else "")
    df_actuals['year'] = df_actuals['Year'] 


    # gdf_counties = gpd.read_parquet('s3://fused-asset/data/tiger/county/tl_rd22_us_county small.parquet')
    result = gdf_sif.merge(df_actuals, on=['year', 'GEOID'], how='left')
    result = gpd.GeoDataFrame(result)
    result.crs = "EPSG:4326"
    
    # TODO: there's some NaN rows, why?
    result['county_area_m2']=result.to_crs(result.estimate_utm_crs()).area
    # _result = result.drop(columns='geometry')
    
    # Convert geometry so it plays well with DuckDB
    result['geometry'] = result['geometry'].apply(lambda geom: geom.wkt)


    out = conn.sql("""
        SELECT 
            ROUND(corn_sif_sum/county_sif_sum, 2) AS m_pct, 
            corn_sif_mean,
            corn_sif_sum,
            county_area_m2,
            (county_area_m2*m_pct) as area_corn_m2,
            (area_corn_m2*.00024711) as area_corn_acres,
            (county_area_m2*00024711) as area_county_acres,
            (area_corn_acres*Value) as bushels_sum_actual,
            year,
            month,
            county,
            GEOID,
            Value as bushels_per_acre_actual,
            period,
            STATE,
            geometry
            
        FROM result 
        WHERE GEOID = '19119'
        LIMIT 1000
    
    """).df()
    
    # Convert geometry back to Python format
    out['geometry'] = out['geometry'].apply(loads)
    # print(out)


    #out table
    out = gpd.GeoDataFrame(out)
    out['date'] = out['month'].astype(str) + out['period'].astype(str)
    print(out.drop(columns='geometry').T)   
    # print(out)
    year_totals = out.groupby(['GEOID', 'year']).agg({
        'bushels_per_acre_actual': 'mean',  # Adjust aggregation logic as needed
        'm_pct': 'mean',
        'bushels_sum_actual': 'mean',       # Example aggregation
        'geometry': 'first',              # Choose how to handle geometry
        'area_county_acres':'mean',
        'area_corn_acres':'mean'
    }).reset_index()
    year_totals = year_totals.drop_duplicates(subset=['GEOID', 'year'])
    print(year_totals.drop(columns='geometry').T)
    
    #Pivot
    pivot = out.pivot_table(
        index=['GEOID','year'], 
        columns=['date'], 
        values=['corn_sif_mean'], 
        aggfunc='first')
    pivot.columns = ['_'.join(filter(None, col)) if isinstance(col, tuple) else col for col in pivot.columns]
    pivot = pivot.rename(columns={'GEOID_': 'GEOID', 'year_': 'year', 'bushels_per_acre_': 'bushels_per_acre'})
    # print(pivot.columns)
    # print(out.columns)
    print(pivot)

    #Make a table that is only year data
    out_reduced = out[['GEOID', 'year', 'bushels_per_acre_actual','area_corn_acres', 'area_county_acres','geometry','m_pct','bushels_sum_actual']]
    # print(out_reduced)
    # print(out_reduced.drop(columns='geometry').T)
    # print(pivot.dtypes)
    # print(year_totals.dtypes)


    # Merge with all the corn sif mean totals
    result = pivot.merge(year_totals, on=['GEOID', 'year'], how='left')

    # Ensure the result is a GeoDataFrame
    result = gpd.GeoDataFrame(result, geometry='geometry')
    # print(result.info())
    # result.rename(columns={'Value': 'bushelsperacre'}, inplace=True)
    print(result)
    print(result.head().drop(columns='geometry').T)
    # print(result.head().drop(columns='geometry'))
    # print(result)
    
    return result

