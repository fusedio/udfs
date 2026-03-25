@fused.udf
def udf(path: str = "s3://fused-sample/demo_data/airbnb_listings_sf.parquet"):
    import pandas as pd
    df = pd.read_parquet(path)
    
    # Remove .head(100) & press "Shift + Enter" to run
    return df.head(100)
