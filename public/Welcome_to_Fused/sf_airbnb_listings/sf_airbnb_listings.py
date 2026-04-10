@fused.udf
def udf():
    import pandas as pd
    
    df = pd.read_parquet("s3://fused-sample/demo_data/airbnb_listings_sf.parquet")
    # Remove .head(100) & press "Shift + Enter" to run
    return df.head(100)
