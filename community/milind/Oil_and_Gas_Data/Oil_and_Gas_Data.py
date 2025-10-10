@fused.udf
def udf(
    path: str = "s3://fused-asset/data/oil_and_gas/oil_and_gas_march_2024.pq"
):
    import pandas as pd
    import fused 

    # Cache the heavy parquet read for faster subsequent calls
    @fused.cache
    def load_parquet(p):
        return pd.read_parquet(p)

    df = load_parquet(path)

    # Debug: print transposed schema to STDOUT
    print(df.T)

    return df