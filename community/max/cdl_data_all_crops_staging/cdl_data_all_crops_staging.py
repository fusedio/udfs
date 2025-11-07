@fused.udf
def udf():
    import duckdb
    
    # Load the common utilities module from Fused's UDF repository
    common = fused.load("https://github.com/fusedio/udfs/tree/fbf5682/public/common/")
    df = duckdb.query(f"""
        SELECT * 
        FROM 's3://fused-asset/misc/hex/CDL_h12k1p1/year=2024/overview/hex5.parquet'
        where 1=1
        and area>1000
    """).to_df()

    df = df.rename(columns={'data': 'crop_type'})
    print(df.T)

    return df