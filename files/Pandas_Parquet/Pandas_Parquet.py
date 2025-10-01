@fused.udf
def udf(path: str):
    import geopandas as gpd
    import pandas as pd

    try:
        # reading differs depending on whether the file is GeoParquet or not
        df = gpd.read_parquet(path)
        df = df.to_crs("EPSG:4326")
    except:
        df = pd.read_parquet(path)
    print(df.T) # transpose the dataframe to make data schema more visible to AI 
    return df
