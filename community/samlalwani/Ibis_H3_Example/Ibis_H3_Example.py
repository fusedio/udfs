# Note: This UDF is only for demo purposes. You may get `HTTP GET error` after several times calling it. This is the data retrieval issue caused by Cloudfront servers not responding.
# Note: This UDF is a copy of the DuckDB_H3_Example
# Modified to use IBIS project. All the original code is kept as is, commented out.

@fused.udf
def udf(bbox=None, resolution: int = 9, min_count: int = 10):
    import shapely
    import geopandas as gpd
    import duckdb
    import ibis
    from ibis import _

    # DuckDB is only used to download extension
    con = duckdb.connect()
    con.sql("INSTALL h3 FROM community;")

    # We use the duckdb extension h3    
    con_ibis = ibis.duckdb.connect(temp_directory='/tmp', allow_unsigned_extensions=True, extensions=['h3'])

    @fused.cache
    def read_data(url, resolution, min_count):
        # df = con.sql("""
        # SELECT h3_h3_to_string(h3_latlng_to_cell(pickup_latitude, pickup_longitude, $resolution)) cell_id,
        #        h3_cell_to_boundary_wkt(cell_id) boundary,
        #        count(1) cnt
        # FROM read_parquet($url)
        # GROUP BY cell_id
        # HAVING cnt>$min_count
        # """, params={'url': url, 'resolution': resolution, 'min_count': min_count}).df()

        sql_str_h3 = f"""
                    SELECT 
                    *, 
                    h3_latlng_to_cell(pickup_latitude, pickup_longitude, {resolution}) AS cell_id,                    
                    FROM tripdata_orig
                    """
        sql_str_boundary = f"""
                            SELECT
                            *,
                            h3_cell_to_boundary_wkt(h3_h3_to_string(cell_id)) AS geometry
                            FROM tripdata_h3
                            """

        gdf = (con_ibis
               .read_parquet(url, table_name='tripdata_orig')
               .sql(sql_str_h3)
               .group_by(['cell_id'])
               .agg(cnt=_.cell_id.count())
               .filter(_.cnt > min_count)
               .alias('tripdata_h3')
               .sql(sql_str_boundary)
               .to_pandas()
               )
        gdf = gpd.GeoDataFrame(gdf, geometry=gpd.GeoSeries.from_wkt(gdf.geometry), crs='epsg:4326')

        return gdf

    url = r'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2010-01.parquet'

    gdf = read_data(url, resolution, min_count)

    print("number of trips using ibis:", gdf.cnt.sum())
    print(gdf)

    return gdf

    print(gdf)

    return gdf
