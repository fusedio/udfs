@fused.udf(cache_max_age=0)
def udf(
    noise_311_link: str = "https://gist.githubusercontent.com/kashuk/670a350ea1f9fc543c3f6916ab392f62/raw/4c5ced45cc94d5b00e3699dd211ad7125ee6c4d3/NYC311_noise.csv",
    res: int = 9
):
    # 1. Load points from above UDF 
    points_udf = fused.load("ny411_noise_points")
    data = points_udf()
    
    # Load common utilities (loads DuckDB with spatial & H3 addons)
    common = fused.load("https://github.com/fusedio/udfs/tree/9bad664/public/common/")
    con = common.duckdb_connect()

    # Remove geometry to prevent error with DuckDB
    del data['geometry']

    # 2. Convert points to H3, 
    # 3. count total number of duplicated H3 cells (assign to `cnt`) & groupby unique H3 cells
    qr = f"""
    SELECT
      h3_latlng_to_cell(lat, lng, {res}) AS hex,
      COUNT(*) AS cnt,
    FROM (SELECT lat, lng FROM data) AS data
    GROUP BY 1
    """

    df = con.sql(qr).df()
    print(df.T)

    return df