@fused.udf
def udf(path: str = "s3://fused-sample/demo_data/airbnb_listings_sf.parquet"):
    import pandas as pd

    @fused.cache
    def load_data(file_path):
        return pd.read_parquet(file_path)

    df = load_data(path)
    print(df.head())
    print(df.dtypes)
    print(df.shape)
    return df