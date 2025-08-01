@fused.udf
def udf(
    bounds: fused.types.Bounds = [-0.113, 51.503, -0.099, 51.513],
    release: str = "2025-06-25-0",
    theme: str = None,
    overture_type: str = None,
    use_columns: list = None,

):
    gdf = get_overture(
        bounds=bounds,
        release=release,
        theme=theme,
        overture_type=overture_type,
        use_columns=use_columns,
    )
    return gdf



def get_overture(
    bounds: fused.types.Bounds = None,
    release: str = "2025-06-25-0",
    theme: str = None,
    overture_type: str = None,
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    clip: bool = None,
    point_convert: str = None,
):
    """Returns Overture data as a GeoDataFrame."""
    import logging
    import concurrent.futures
    import json

    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import shape, box

    common = fused.load("https://github.com/fusedio/udfs/tree/364f5dd/public/common/")

    if release == "2024-02-15-alpha-0":
        if overture_type == "administrative_boundary":
            overture_type = "administrativeBoundary"
        elif overture_type == "land_use":
            overture_type = "landUse"
        theme_per_type = {
            "building": "buildings",
            "administrativeBoundary": "admins",
            "place": "places",
            "landUse": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }
    elif release == "2024-03-12-alpha-0":
        theme_per_type = {
            "building": "buildings",
            "administrative_boundary": "admins",
            "place": "places",
            "land_use": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }
    else:
        theme_per_type = {
            "address": "addresses",
            "building": "buildings",
            "infrastructure": "base",
            "land": "base",
            "land_use": "base",
            "land_cover": "base",
            "water": "base",
            "bathymetry": "base",
            "place": "places",
            "division": "divisions",
            "division_area": "divisions",
            "division_boundary": "divisions",
            "segment": "transportation",
            "connector": "transportation",
        }

    if theme is None:
        theme = theme_per_type.get(overture_type, "buildings")

    if overture_type is None:
        type_per_theme = {v: k for k, v in theme_per_type.items()}
        overture_type = type_per_theme[theme]

    if num_parts is None:
        if overture_type == "building":
            if release >= "2025-01-22-0":
                num_parts = 6
            else:
                num_parts = 5
        else:
            num_parts = 1

    if min_zoom is None:
        if theme == "admins" or theme == "divisions":
            min_zoom = 7
        elif theme == "base":
            min_zoom = 9
        else:
            min_zoom = 12

    table_path = f"s3://us-west-2.opendata.source.coop/fused/overture/{release}/theme={theme}/type={overture_type}"
    table_path = table_path.rstrip("/")

    def get_part(part):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            return common.table_to_tile(bounds, table=part_path, use_columns=use_columns, min_zoom=min_zoom)
        except ValueError:
            return None

    if num_parts > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parts) as pool:
            dfs = list(pool.map(get_part, range(num_parts)))
    else:
        # Don't bother creating a thread pool to do one thing
        dfs = [get_part(0)]

    dfs = [df for df in dfs if df is not None]

    if len(dfs):
        gdf = pd.concat(dfs)

    else:
        print("Failed to get any data")
        return None

    if point_convert is not None:
        gdf["geometry"] = gdf.geometry.centroid

    if clip:
        gdf = gdf.clip(bounds)

    return gdf
