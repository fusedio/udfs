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
    Returns airport data as a GeoDataFrame with a color property that reflects
    the airport type (large, medium, small, heliport). The Workbench will render
    the points on the map using this color.
    """
    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import Point

    # Load CSV
    df = pd.read_csv(path)

    # Build geometry column
    geometry = [Point(xy) for xy in zip(df.longitude_deg, df.latitude_deg)]
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    # Apply optional filters
    if airport_type:
        gdf = gdf[gdf.type == airport_type]
    if continent:
        gdf = gdf[gdf.continent == continent]
    if min_elevation:
        gdf = gdf[gdf.elevation_ft >= min_elevation]
    if scheduled_service:
        gdf = gdf[gdf.scheduled_service == scheduled_service]

    # Spatial filter using bounds if provided
    if bounds is not None:
        from shapely.geometry import box

        bbox = box(*bounds)
        gdf = gdf[gdf.geometry.intersects(bbox)]

    # Keep only requested columns
    if use_columns:
        keep_cols = list(set(use_columns + ["geometry"]))
        gdf = gdf[keep_cols]

    # ------------------------------------------------------------------
    # Add a colour column based on airport type for map rendering
    # ------------------------------------------------------------------
    colour_map = {
        "large_airport": "#ff0000",   # red
        "medium_airport": "#ffa500",  # orange
        "small_airport": "#00ff00",   # green
        "heliport": "#0000ff",        # blue
    }
    # Use hex colour strings; unknown types get a default grey
    gdf["color"] = gdf["type"].map(colour_map).fillna("#808080")

    # Debug print of the final GeoDataFrame schema
    print(gdf.T)

    # Return the GeoDataFrame; Workbench will display it on the map,
    # using the 'color' property for point styling.
    return gdf