@fused.udf
def udf(path: str):
    common = fused.load("https://github.com/fusedio/udfs/tree/4dde28e/public/common/")
    con = common.duckdb_connect()
    df = con.sql(f"SELECT * FROM read_parquet('{path}') limit 100_000").df()
    print(df.T) # transpose the dataframe to make data schema more visible to AI 
    return df
