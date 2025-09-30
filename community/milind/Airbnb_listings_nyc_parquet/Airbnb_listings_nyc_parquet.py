@fused.udf
def udf(path: str='s3://fused-sample/demo_data/airbnb_listings_nyc.parquet'):
    import pandas as pd
    import numpy as np


    df = pd.read_parquet(path)

    # calculate price per person
    if {"price_in_dollar", "accommodates"}.issubset(df.columns):
        df["price_per_person"] = np.where(
            (df["accommodates"] > 0) & df["price_in_dollar"].notna(),
            df["price_in_dollar"] / df["accommodates"],
            np.nan
        )

    # keep only useful columns
    keep_cols = [
        "name",
        "neighbourhood_cleansed",
        "price_per_person",
        "latitude", "longitude"
    ]
    df = df[[c for c in keep_cols if c in df.columns]]

    return df
