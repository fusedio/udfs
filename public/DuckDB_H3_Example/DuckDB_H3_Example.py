# Note: This UDF is only for demo purposes. You may get `HTTP GET error` after several times calling it. This is the data retrieval issue caused by Cloudfront servers not responding.
@fused.udf
def udf(bounds: fused.types.TileGDF = None, resolution: int = 9, min_count: int = 10):
    import duckdb
    from utils import duckdb_with_h3
    import shapely
    import geopandas as gpd

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils
    con = duckdb_with_h3()

    con.sql("INSTALL httpfs; LOAD httpfs;")
    
    @fused.cache
    def read_data(url, resolution, min_count):
        df = con.sql("""
        SELECT h3_h3_to_string(h3_latlng_to_cell(pickup_latitude, pickup_longitude, $resolution)) cell_id,
               h3_cell_to_boundary_wkt(cell_id) boundary,
               count(1) cnt
        FROM read_parquet($url) 
        GROUP BY cell_id
        HAVING cnt>$min_count
        """, params={'url': url, 'resolution': resolution, 'min_count': min_count}).df()
        return df

    df = read_data('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2010-01.parquet', resolution, min_count)
    print("number of trips:", df.cnt.sum())
    gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    print(gdf)
    return gdf
