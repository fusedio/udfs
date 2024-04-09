@fused.udf
def udf(bbox: fused.types.TileGDF=None, resolution: int = 11, min_count: int = 10):
    import duckdb
    import shapely
    import geopandas as gpd
    import h3

    tile_bbox_gdf = gpd.GeoDataFrame.from_features({"type":"FeatureCollection","features":[{"type":"Feature","properties":{"shape":"Rectangle"},"geometry":{"type":"Polygon","coordinates":[[[-73.99322955922597,40.76627870054801],[-73.96753345042097,40.76627870054801],[-73.96753345042097,40.74825844008337],[-73.99322955922597,40.74825844008337],[-73.99322955922597,40.76627870054801]]]}}]})
    default_bbox = tile_bbox_gdf.iloc[0].geometry
    tile_bbox_geom = bbox if bbox is not None else default_bbox

    bounds = bbox.bounds.values[0] if bbox is not None else default_bbox.bounds
    print(bounds)

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils
    h3_utils = fused.load(
        "https://github.com/fusedio/udfs/tree/ebd4937/public/DuckDB_H3_Example/"
    ).utils
    con = duckdb.connect(config = {'allow_unsigned_extensions': True})

    h3_utils.load_h3_duckdb(con)
    con.sql(f"""INSTALL httpfs; LOAD httpfs;""")
    
    @fused.cache
    def read_data(url, resolution, min_count, bounds):
        xmin, ymin, xmax, ymax = bounds
        
        query = """
        SELECT h3_h3_to_string(h3_latlng_to_cell(pickup_latitude, pickup_longitude, $resolution)) cell_id,
               h3_cell_to_boundary_wkt(cell_id) boundary,
               h3_cell_to_lat(cell_id) cell_lat,
               h3_cell_to_lng(cell_id) cell_lng,
               count(1) cnt
        FROM read_parquet($url) 
        WHERE cell_lat >= $ymin
        AND cell_lat < $ymax
        AND cell_lng >= $xmin
        AND cell_lng < $xmax
        GROUP BY cell_id
        HAVING cnt>$min_count
        """
        print(query)

        df = con.sql(query, params={'url': url, 'xmin': xmin, 'xmax': xmax, 'ymin': ymin, "ymax": ymax, 'min_count': min_count, 'resolution': resolution}).df()
        return df


    df = read_data(
        url='s3://fused-asset/misc/nyc/tlc/trip-data/yellow_tripdata_2010-01.parquet', 
        resolution=resolution, 
        min_count=min_count, 
        bounds=bounds
    )
    print("number of trips:", df.cnt.sum())
    gdf = gpd.GeoDataFrame(df.drop(columns=['boundary']), geometry=df.boundary.apply(shapely.wkt.loads))
    print(gdf)
    return gdf
