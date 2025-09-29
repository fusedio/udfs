@fused.udf
def udf(path: str='s3://fused-sample/demo_data/airbnb_listings_nyc.parquet'):
    import geopandas as gpd
    import pandas as pd
    import numpy as np

    # Load with geopandas if geometry exists, else fallback to pandas
    try:
        df = gpd.read_parquet(path)
        try:
            df = df.to_crs('EPSG:4326')
        except Exception:
            pass
    except Exception:
        df = pd.read_parquet(path)

    # calculating price per person
    if {"price_in_dollar", "accommodates"}.issubset(df.columns):
        df["price_per_person"] = np.where(
            (df["accommodates"] > 0) & df["price_in_dollar"].notna(),
            df["price_in_dollar"] / df["accommodates"],
            np.nan
        )


    return df.head()
