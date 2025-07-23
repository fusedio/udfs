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
        return gpd.GeoDataFrame.from_features(result)

    # TODO: make it async
    L = time_steps
    chunk = (len(L) - 1) // 4 + 1
    a = []
    for i in range(chunk):
        a.append(_get_isochrone(lat, lng, costing, time_steps=L[4 * i : 4 * (i + 1)]))
    gdf = pd.concat(a)
    return gdf
