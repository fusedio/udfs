# Note: This UDF is only for demo purposes. You may get `HTTP GET error` after several times calling it. This is the data retrieval issue caused by Cloudfront servers not responding.
@fused.udf
def udf(bbox=None, resolution: int = 9, min_count: int = 10):
    import duckdb
    import shapely
    import geopandas as gpd

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils
    h3_utils = fused.load(
        "https://github.com/fusedio/udfs/tree/870e162/public/DuckDB_H3_Example/"
    ).utils
    con = duckdb.connect()

    h3_utils.load_h3_duckdb(con)
    con.sql(f"""INSTALL httpfs; LOAD httpfs;""")
    
    @fused.cache
    def read_data(url, resolution, min_count):
        df = con.sql("""
        SELECT h3_h3_to_string(h3_latlng_to_cell(pickup_latitude, pickup_longitude, $resolution)) cell_id,
               count(1) cnt
        FROM read_parquet($url) 
        GROUP BY cell_id
        HAVING cnt>$min_count
        """, params={'url': url, 'resolution': resolution, 'min_count': min_count}).df()
        return df

    df = read_data('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2010-01.parquet', resolution, min_count)
    print("number of trips:", df.cnt.sum())
    print(df)
    return df
