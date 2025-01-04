import fused


def duckdb_with_h3(extra_config=None, extra_connect_args=None):
    import duckdb

    con = duckdb.connect(
        config={
            "allow_unsigned_extensions": True,
            **(extra_config if extra_config is not None else {}),
        },
        **(extra_connect_args if extra_connect_args is not None else {}),
    )
    load_h3_duckdb(con)
    return con


def load_h3_duckdb(con):
    import duckdb

    new_home_path = fused.core.create_path(f"duckdb/{duckdb.__version__}/")
    con.sql(f"SET home_directory='{new_home_path}';")
    con.sql("INSTALL spatial; INSTALL httpfs")
    con.sql("INSTALL h3 FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';")
    con.sql("LOAD h3;")

import fused


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