@fused.udf()
def udf(
    path: str = "s3://fused-sample/demo_data/airports.csv",
    bounds: fused.types.Bounds = None,
    airport_type: str = None,
    continent: str = None,
    min_elevation: float = None,
    scheduled_service: str = None,
    use_columns: list = None,
):
    """
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
        # If bounds is a list of four coordinates [minx, miny, maxx, maxy],
        # create a bounding box polygon and filter geometries that intersect it.
        from shapely.geometry import box
        bbox = box(*bounds)
        gdf = gdf[gdf.geometry.intersects(bbox)]

    if use_columns:
        keep_cols = list(set(use_columns + ["geometry"]))
        gdf = gdf[keep_cols]

    print(gdf.T)

    return gdf
