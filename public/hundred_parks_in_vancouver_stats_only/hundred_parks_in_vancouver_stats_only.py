@fused.udf
def udf():
    """
    UDF to get the polygon geometries of parks in Vancouver based on Open Data Portal
    """
    import requests
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Polygon, MultiPolygon, shape
    import numpy as np
    import math
    from pandas import json_normalize

    @fused.cache
    def get_request(url):
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response

    limit = 100
    parks_url = f"https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/parks-polygon-representation/records?limit={str(limit)}"
    response = get_request(url=parks_url)
    json_data = response.json()
    
    # First extract all non-geometry data
    df = json_normalize(json_data['results'])
    
    # Drop the geom column which will be replaced with proper geometry
    if 'geom' in df.columns:
        df = df.drop(columns=['geom'])

    # Filter for valid polygons or multipolygons
    valid_items = [
        item for item in json_data['results']
        if 'geom' in item 
        and item['geom'] is not None
        and isinstance(item['geom'], dict)
        and 'geometry' in item['geom'] 
        and item['geom']['geometry'] is not None
        and isinstance(item['geom']['geometry'], dict)
        and 'type' in item['geom']['geometry']
        and item['geom']['geometry']['type'] in ['Polygon', 'MultiPolygon']
    ]
    
    # Create geometries using shapely's shape function
    geometries = []
    for item in valid_items:
        try:
            geom = shape(item['geom']['geometry'])
            geometries.append(geom)
        except Exception as e:
            print(f"Error creating geometry: {e}")
            continue
    
    # Create filtered dataframe with matching indices
    filtered_df = pd.DataFrame(valid_items).drop(columns=['geom'])
    
    # Create GeoDataFrame with geometries
    gdf = gpd.GeoDataFrame(
        filtered_df, 
        geometry=geometries,
        crs="EPSG:4326"
    )

    # Adding estimate of average size of park so next UDF knows bu how much it needs to buffer lat / lon point to get similar expecation
    gdf['buffer_radius'] = gdf['area_ha'].apply(
        lambda x: math.sqrt((x * 10000) / math.pi)
    )

    # Removing geometry from being passed so Claude deals with less data
    del gdf['geometry']
    del gdf['park_url']
    del gdf['park_id']

    print(gdf.sample(4))
    print(gdf.columns)

    return gdf