@fused.udf
def udf(
    path: str = "s3://fused-sample/demo_data/airbnb_listings_sf.parquet"
):
    """
    Load the Airbnb listings dataset for San Francisco from the Fused sample data
    and return it as a Pandas DataFrame.
    """
    import pandas as pd
    import fused

    # Cache the heavy parquet read for faster subsequent runs
    @fused.cache
    def load_data(file_path: str):
        return pd.read_parquet(file_path)

    df = load_data(path)

    # Debugging: print the transposed schema of the DataFrame
    print(df.T)

    return df