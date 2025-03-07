@fused.udf
def udf(bounds: fused.types.TileGDF = None, crs="EPSG:4326", res=7):
    import fused
    import pandas as pd
    import geopandas as gpd
    import h3
    import requests
    from utils import fetch_all_features, add_rgb_cmap, CMAP


    # Generate ESRI-friendly envelope bounds
    total_bounds = bounds.geometry.total_bounds
    envelope = f'{total_bounds[0]},{total_bounds[1]},{total_bounds[2]},{total_bounds[3]}'
    
    # URL for querying the Watch/Warning/Advisory (WWA) FeatureServer
    url = "https://mapservices.weather.noaa.gov/eventdriven/rest/services/WWA/watch_warn_adv/FeatureServer/1/query?"

   # Define the parameters for the query
    params = {
        "geometryType": "esriGeometryEnvelope",  # Type of geometry to use in the spatial query (for bounds)
        "geometry": envelope,                    # The bounding box geometry for the query
        "inSR": "4326",                          # The spatial reference of the input geometry
        "spatialRel": "esriSpatialRelIntersects",# Spatial relationship rule for the query
        "returnGeometry": "true",                # Whether to return geometry of features
        "where": "1=1",                          # SQL where clause to filter features (no filter here)
        "maxRecordCount": 500,                   # Maximum number of records to return per request
        "outFields": "*",                        # Fields to return in the response (all fields)
        "outSR": "4326",                         # The spatial reference of the output geometry
        "f": "geojson",                          # Format of the response (GeoJSON)
    }


    # Fetch all data using pagination if needed
    features = fetch_all_features(url, params)
    if not features:
        # No features found or an error occurred
        return None

    # Convert the GeoJSON data to a GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(features, crs=crs)
    
    # Convert GeoDataFrame geos to h3 cells
    cell_column = pd.Series([h3.geo_to_cells(geom, res=res) for geom in gdf.geometry])
    shape_column = cell_column.apply(h3.cells_to_h3shape)
    gdf.geometry = shape_column

    # Add 'r', 'g', and 'b' fields to the GeoDataFrame
    gdf = add_rgb_cmap(gdf=gdf, key_field="prod_type", cmap_dict=CMAP)
    
    return gdf
