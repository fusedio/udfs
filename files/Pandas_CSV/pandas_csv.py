@fused.udf
def udf(path: str):
    import geopandas as gpd
    import pandas as pd

    df = pd.read_csv(path)
    print(df)
    return df
