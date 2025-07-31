import pandas as pd

@fused.udf
def udf(locations: pd.DataFrame = None, h3_res: int = 11, distance: int = 10):
    import json
    import geopandas as gpd
    import shapely
    from shapely.geometry import Polygon, MultiPolygon
    common = fused.load("https://github.com/fusedio/udfs/tree/3670b6b/public/common/")
    import duckdb
    con = common.duckdb_connect()

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
        df = con.sql(f"""WITH cells AS (SELECT  
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


@fused.cache
def get_isochrone(
    lat=40.743, lng=-73.945, costing="auto", time_steps=[1, 3, 5, 7, 10, 15, 20, 30]
):
    """
    costing options: auto, pedestrian, bicycle, truck, bus, motor_scooter
    TODO: add costing_options: e.g. exclude_polygons
    """
    import geopandas as gpd
    import pandas as pd
    import requests
    from shapely.geometry import MultiPolygon

    def _get_isochrone(lat, lng, costing, time_steps):
        import random
        import time

        url = "https://valhalla1.openstreetmap.de/isochrone"
        params = {
            "locations": [{"lon": lng, "lat": lat}],
            "contours": [{"time": i} for i in time_steps],
            "costing": costing,
            "polygons": 1,
        }
        response = requests.post(url, json=params)
        result = response.json()
        
        if 'error' not in result:
            return gpd.GeoDataFrame.from_features(result).geometry.iloc[0]
        else:
            return None

    # TODO: make it async
    L = time_steps
    chunk = (len(L) - 1) // 4 + 1
    a = []
    for i in range(chunk):
        result = _get_isochrone(lat, lng, costing, time_steps=L[4 * i : 4 * (i + 1)])
        if result is not None:
            a.append(result)
    if len(a) > 0:
        return a[0]
    else:
        return None