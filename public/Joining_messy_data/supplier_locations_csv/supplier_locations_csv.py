@fused.udf
def udf():
    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import Point

    path = 's3://fused-asset/demos/supplier_customer_joining/supplier_locations.csv'

    df = pd.read_csv(path)

    # Detect lat/lon columns (case-insensitive)
    col_map = {c.lower(): c for c in df.columns}
    lat_col = col_map.get('latitude') or col_map.get('lat')
    lon_col = col_map.get('longitude') or col_map.get('lon') or col_map.get('lng')

    geometry = [Point(xy) for xy in zip(df[lon_col], df[lat_col])]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    return gdf
    