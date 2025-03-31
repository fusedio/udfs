@fused.udf
def udf(
    path: str = 's3://fused-users/fused/aaryan/airports.csv',
    bbox: fused.types.TileGDF = None,
    airport_type: str = None,
    continent: str = None,
    min_elevation: float = None,
    scheduled_service: str = None,
    use_columns: list = None
):
    """
    Returns airport datasda as a GeoDataFrame with visualization parameters.
    """
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Point
    
    # Read airports CSV
    df = pd.read_csv(path)
    
    # Create geometry from lat/lon
    geometry = [Point(xy) for xy in zip(df.longitude_deg, df.latitude_deg)]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)
    
    # Apply filters if provided
    if airport_type:
        gdf = gdf[gdf.type == airport_type]
    if continent:
        gdf = gdf[gdf.continent == continent]
    if min_elevation:
        gdf = gdf[gdf.elevation_ft >= min_elevation]
    if scheduled_service:
        gdf = gdf[gdf.scheduled_service == scheduled_service]
        
    # Spatial filter if bbox provided
    if bbox is not None:
        gdf = gdf[gdf.geometry.intersects(bbox.geometry.iloc[0])]
        
    # Select columns
    if use_columns:
        keep_cols = list(set(use_columns + ['geometry']))
        gdf = gdf[keep_cols]
    
    print(gdf)
    return gdf