@fused.udf
def udf(
    noise_311_link: str = "https://gist.githubusercontent.com/kashuk/670a350ea1f9fc543c3f6916ab392f62/raw/4c5ced45cc94d5b00e3699dd211ad7125ee6c4d3/NYC311_noise.csv",
    res: int = 9
):
    # Load common utilities (includes duckdb helper)
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    con = common.duckdb_connect()

    qr = f"""
    SELECT
      h3_latlng_to_cell(lat, lng, {res}) AS hex,
      COUNT(*) AS cnt
    FROM read_csv_auto('{noise_311_link}')
    WHERE lat IS NOT NULL AND lng IS NOT NULL
    GROUP BY 1
    """

    df = con.sql(qr).df()

    # Debugging: print the resulting DataFrame schema
    print(df.T)

    return df