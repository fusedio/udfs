@fused.udf
def udf():
    import geopandas as gpd
    import requests

    DATASET = "https://raw.githubusercontent.com/python-visualization/folium-example-data/main/subway_stations.geojson"
    gdf = gpd.GeoDataFrame.from_features(requests.get(DATASET).json())

    return gdf
