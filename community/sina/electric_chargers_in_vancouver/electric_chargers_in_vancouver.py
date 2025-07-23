@fused.udf
def udf():
    """
    UDF to get the location of electric chargers around Vancouver based on Open Data Portal
    """
    import requests
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Point
    import numpy as np
    from pandas import json_normalize

    @fused.cache
    def get_request(url):
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response

    limit = 100
    building_permits_url = f"https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/electric-vehicle-charging-stations/records?limit={str(limit)}"
    response = get_request(url=building_permits_url)
    json_data = response.json()
    
    # First extract all non-geometry data
    df = json_normalize(json_data['results'])
    
    # Drop the geom column which will be replaced with proper geometry
    if 'geom' in df.columns:
        df = df.drop(columns=['geom'])

    # Skipping any point that doesn't have valid geom
    valid_items = [
        item for item in json_data['results']
        if 'geom' in item 
        and item['geom'] is not None
        and isinstance(item['geom'], dict)
        and 'geometry' in item['geom'] 
        and item['geom']['geometry'] is not None
        and isinstance(item['geom']['geometry'], dict)
        and 'type' in item['geom']['geometry']
        and item['geom']['geometry']['type'] == 'Point'
    ]
    
    # Extract coordinates directly into arrays
    coords = np.array([
        item['geom']['geometry']['coordinates'] 
        for item in valid_items
    ])
    
    # Create Points in a vectorized way
    geometries = [Point(x, y) for x, y in coords]
    
    # Create filtered dataframe with matching indices
    filtered_df = pd.DataFrame(valid_items).drop(columns=['geom'])
    
    # Create GeoDataFrame with geometries
    gdf = gpd.GeoDataFrame(
        filtered_df, 
        geometry=geometries,
        crs="EPSG:4326"
    )

    # Keeping output light for Claude
    del gdf['geometry']
    
    print(f"{gdf.sample=}")
    return gdf
    
