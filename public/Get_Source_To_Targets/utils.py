import fused


@fused.cache
def get_sources_to_targets(
    sources=[[40.743,-73.945]], targets=[[40.742,-73.944]], costing="pedestrian"):
    """
    costing options: auto, pedestrian, bicycle, truck, bus, motor_scooter
    please limit the source and target combination to ~25
    https://valhalla.github.io/valhalla/api/matrix/api-reference/
    """
    import geopandas as gpd
    import pandas as pd
    import requests

    url = "https://valhalla1.openstreetmap.de/sources_to_targets"
    params = {"sources":[{"lat":lat,"lon":lon} for lat, lon in sources],"targets":[{"lat":lat,"lon":lon} for lat, lon in targets],"costing":costing}

    response = requests.post(url, json=params)
    result = pd.DataFrame(response.json()['sources_to_targets'][0])

    return result
