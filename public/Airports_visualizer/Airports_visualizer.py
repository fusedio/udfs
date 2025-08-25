@fused.udf()
def udf(
    path: str = "s3://fused-users/fused/aaryan/airports.csv",
    bounds: fused.types.TileGDF = None,
    airport_type: str = None,
    continent: str = None,
    min_elevation: float = None,
    scheduled_service: str = None,
    use_columns: list = None,
):
    """
    Made a change from Github - I'm a team member changing this code
    Saving this through a PR in Github
    Returns airport data as a GeoDataFrame with visualization parameters.
    """
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Point

    df = pd.read_csv(path)

    


    

    
    geometry = [Point(xy) for xy in zip(df.longitude_deg, df.latitude_deg)]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    if airport_type:
        gdf = gdf[gdf.type == airport_type]
    if continent:
        gdf = gdf[gdf.continent == continent]
    if min_elevation:
        gdf = gdf[gdf.elevation_ft >= min_elevation]
    if scheduled_service:
        gdf = gdf[gdf.scheduled_service == scheduled_service]

    if bounds is not None:
        gdf = gdf[gdf.geometry.intersects(bounds.geometry.iloc[0])]

    if use_columns:
        keep_cols = list(set(use_columns + ["geometry"]))
        gdf = gdf[keep_cols]

    return gdf
