@fused.udf
def udf():
    import pandas as pd

    @fused.cache
    def load_data():
        return pd.read_parquet("s3://fused-sample/demo_data/airbnb_listings_sf.parquet")

    df = load_data()
    print(df.head())
    print(df.dtypes)
    print(df.shape)
    return df