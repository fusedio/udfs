@fused.udf
def udf():
    import geopandas as gpd
    import shapely
    from shapely import wkt
    from shapely.wkt import loads
    import pandas as pd
    import duckdb
    import numpy as np
    import matplotlib.pyplot as plt
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import mean_squared_error



    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    con = duckdb.connect()  
    
    @fused.cache
    def load_sif_data():
        table = 's3://fused-asset/misc/plinio/sif_output/county=*/year=*/month=*/*.parquet'
        df = con.sql(f"""SELECT * FROM read_parquet('{table}',hive_partitioning = true)""").df()
        df['geometry'] = df['geometry'].apply(loads)
        gdf_sif = gpd.GeoDataFrame(df)
        return gdf_sif

    # Load SIF data
    gdf_sif = load_sif_data()

    # Load Yield actuals data
    df_actuals = load_target_data()
    df_actuals['GEOID'] = df_actuals['State ANSI'].astype(str).str.zfill(2) + df_actuals['County ANSI'].apply(lambda x: str(int(x)).zfill(3) if not pd.isna(x) else "")
    df_actuals['year'] = df_actuals['Year'] 

    # Structure table
    result = gdf_sif.merge(df_actuals, on=['year', 'GEOID'], how='left')
    result = gpd.GeoDataFrame(result)
    result.crs = "EPSG:4326"
    
    # TODO: address NaN rows
    result['county_area_m2']=result.to_crs(result.estimate_utm_crs()).area
    
    # Convert geometry so it plays well with DuckDB
    result['geometry'] = result['geometry'].to_wkt()
    
    # Structure training table with DuckDB
    out = con.sql("""
        SELECT 
            ROUND(corn_sif_sum/county_sif_sum, 2) AS m_pct, 
            corn_sif_mean,
            corn_sif_sum,
            county_area_m2,
            (county_area_m2*m_pct) as area_corn_m2,
            (area_corn_m2*.00024711) as area_corn_acres,
            (county_area_m2*00024711) as area_county_acres,
            (area_corn_acres*Value) as bushels_area_corn,
            year,
            month,
            county,
            GEOID,
            Value as bushels_per_acre,
            period,
            STATE,
            geometry
            
        FROM result 
        ORDER BY m_pct DESC
        LIMIT 1000
    
    """).df()
    
    # Convert geometry back to Python format
    out['geometry'] = shapely.from_wkt(out['geometry'])
    out = gpd.GeoDataFrame(out)
    out['date'] = out['month'].astype(str) + out['period'].astype(str)

    #Pivot
    pivot = out.pivot_table(
        index=['GEOID','year','bushels_per_acre'], 
        columns=['date'], 
        values=['corn_sif_mean'], 
        aggfunc='first')
    pivot.columns = ['_'.join(filter(None, col)) if isinstance(col, tuple) else col for col in pivot.columns]
    pivot = pivot.rename(columns={'GEOID_': 'GEOID', 'year_': 'year', 'bushels_per_acre_': 'bushels_per_acre'})
    out_reduced = out[['GEOID', 'year', 'bushels_per_acre','area_corn_acres', 'area_county_acres','geometry','m_pct','bushels_area_corn']]
    result = pivot.merge(out_reduced, on=['GEOID', 'year'], how='left')

    # Ensure the result is a GeoDataFrame
    result = gpd.GeoDataFrame(result, geometry='geometry')
    result.rename(columns={'Value': 'bushelsperacre'}, inplace=True)
    
    print(result.head().drop(columns='geometry').T)
    
    return result


@fused.cache
def load_target_data():
    import pandas as pd
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

