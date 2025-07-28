@fused.udf
def udf(locations: pd.DataFrame = None, h3_res: int = 11, distance: int = 10):
    import pandas as pd
    import json
    import geopandas as gpd
    import shapely
    from shapely.geometry import Polygon, MultiPolygon
    common = fused.load(
    "https://github.com/fusedio/udfs/tree/f8f0c0f/public/common/"
    ).utils

    import duckdb
    from utils import duckdb_with_h3, get_isochrone
    con = duckdb_with_h3()

    if locations is None:
        df = pd.DataFrame({'x': [37.7766831, 37.736823, 37.756823], 'y': [-122.4073279,-122.4101689,-122.1101689]})

    df['geometry'] = df.apply(lambda x: get_isochrone(x['x'], x['y'], costing="auto", time_steps=[distance]), axis=1)
    
    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    con.sql("LOAD httpfs; LOAD spatial;")
    if len(df) > 0:
        gdf['wkt'] = gdf.geometry.to_wkt()

        del gdf['geometry']
        print(gdf)
        con.sql("CREATE TABLE my_table AS SELECT * FROM gdf")
        df = con.sql(f"""WITH cells AS (SELECT  \
                    DISTINCT(UNNEST(h3_polygon_wkt_to_cells(wkt, {h3_res}))) cell_id
                   FROM my_table),
                   h3_smooth AS (
                   SELECT DISTINCT(h3_cell_to_parent(cell_id, 8)) boundary FROM cells)
                   SELECT ST_AsGeoJSON(ST_GEOMFROMTEXT(h3_cells_to_multi_polygon_wkt(ARRAY_AGG(boundary)))) boundary from h3_smooth
                   """).df()

        df['geometry'] = df.boundary.apply(json.loads)
        del df['boundary']
        df['geometry'] = df.apply(lambda x: MultiPolygon([Polygon([c for c in p][0] + x.geometry['coordinates'][i][0]) for (i, p) in enumerate(x.geometry['coordinates'])]), axis=1)

        df = gpd.GeoDataFrame(df, geometry='geometry')

    return df