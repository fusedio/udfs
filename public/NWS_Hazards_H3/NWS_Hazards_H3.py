@fused.udf
def udf(bbox: fused.types.TileGDF = None, crs="EPSG:4326", res=7):
    import fused
    import geopandas as gpd
    import h3
    import requests
    from utils import CMAP, add_rgb_cmap

    # URL for querying the WatchesWarnings layer of the Watch/Warning/Advisory (WWA) MapServer
    url = "https://mapservices.weather.noaa.gov/eventdriven/rest/services/WWA/watch_warn_adv/MapServer/1/query"

    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "geometryType": "esriGeometryEnvelope",
        # "geometry": bbox,
        "inSR": "4326",
        "spatialRel": "esriSpatialRelEnvelopeIntersects",
    }

    # Fetch data
    @fused.cache
    def fetch_data(params):
        return requests.get(url, params=params)

    response = fetch_data(params)
    data = response.json()

    # Convert the GeoJSON data to a GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(data["features"], crs=crs)

    # Convert GeoDataFrame geos to h3 cells
    cell_column = gdf.geometry.apply(lambda x: h3.geo_to_cells(x, res=res))
    shape_column = cell_column.apply(h3.cells_to_h3shape)
    gdf.geometry = shape_column

    # Add 'r', 'g', and 'b' fields to the GeoDataFrame
    gdf = add_rgb_cmap(gdf=gdf, key_field="prod_type", cmap_dict=CMAP)

    return gdf
