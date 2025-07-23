# Note: This UDF is only for demo purposes. You may get `HTTP GET error` after several times calling it. This is the data retrieval issue caused by Cloudfront servers not responding.
@fused.udf
def udf(agg_factor=3, min_count=5):
    import duckdb

    utils = fused.load("https://github.com/fusedio/udfs/tree/3569595/public/common/").utils
    old_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    con = utils.duckdb_connect()

    print("duckdb version:", duckdb.__version__)
    df = con.sql(
        f"""
    SELECT round(pickup_longitude*{agg_factor},3)/{agg_factor} lng, 
           round(pickup_latitude*{agg_factor},3)/{agg_factor} lat, 
           count(1) cnt
    FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2010-01.parquet') 
    GROUP BY round(pickup_longitude*{agg_factor},3), 
             round(pickup_latitude*{agg_factor},3)
    HAVING cnt>{min_count} and lat>40 and lat<41
    """
    ).df()
    print("number of trips:", df.cnt.sum())
    gdf = old_utils.geo_convert(df)
    return gdf
