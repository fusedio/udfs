@fused.udf
def udf(path: str):
    import duckdb

    con = duckdb.connect()

    con.sql("install 'httpfs'; load 'httpfs';")
    df = con.sql(f"SELECT * FROM read_csv('{path}')").df()
    print(df.T) # transpose the dataframe to make data schema more visible to AI 
    return df
