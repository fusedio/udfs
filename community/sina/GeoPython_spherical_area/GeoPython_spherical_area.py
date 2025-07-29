@fused.udf
def udf(bounds: fused.types.Bounds = [7.634,47.528,7.651,47.540]):
    # adding custom path for spherely (not yet installed by default)
    import sys;
    sys.path.append(f"/mnt/cache/envs/geopython/lib/python3.11/site-packages")

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, clip=True)

    # loading the Overture building polygons for the current bounds
    gdf = get_overture(tile, theme="buildings", use_columns=["geometry"])
    gdf = gdf[["geometry"]]

    # constructing spherely Geography objects through WKB
    import spherely
    geogs = spherely.from_wkb(gdf.geometry.to_wkb())

    # Calculating the area using shapely by reprojecting to UTM first
    geoms_utm = gdf.geometry.to_crs(gdf.estimate_utm_crs())
    gdf["area_utm"] = geoms_utm.area

    # Calculating the area using spherely
    gdf["area_spherical"] = spherely.area(geogs)

    # Calculating the geodesic area using pyproj (ellipsoid model)
    from pyproj import Geod
    geod = Geod(ellps="WGS84")
    gdf["area_geod"] = [geod.geometry_area_perimeter(pol)[0] for pol in gdf.geometry]

    print("Average relative difference compared to geodesic area:")
    print(f'Spherical: {((gdf["area_spherical"] - gdf["area_geod"]) / gdf["area_geod"] * 100).mean():.2f}%')
    print(f'UTM: {((gdf["area_utm"] - gdf["area_geod"]) / gdf["area_geod"] * 100).mean():.2f}%')

    return gdf


@fused.cache
def get_overture(
    bounds: fused.types.Bounds = None,
    release: str = "2024-10-23-0",
    theme: str = None,
    overture_type: str = None,
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: str = None,
    point_convert: str = None
):
    """Returns Overture data as a GeoDataFrame."""
    import logging
    import concurrent.futures
    import json

    import geopandas as gpd
    import pandas as pd
    from shapely.geometry import shape, box

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds)

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
            "water": "base",
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
        num_parts = 1 if overture_type != "building" else 5

    if min_zoom is None:
        if theme == "admins" or theme == "divisions":
            min_zoom = 7
        elif theme == "base":
            min_zoom = 9
        else:
            min_zoom = 12

    table_path = f"s3://us-west-2.opendata.source.coop/fused/overture/{release}/theme={theme}/type={overture_type}"
    table_path = table_path.rstrip("/")

    if polygon is not None:
        polygon=gpd.from_features(json.loads(polygon))
        tile = polygon.geometry.bounds
        tile = gpd.GeoDataFrame(
            {
                "geometry": [
                    box(
                        bounds[0],
                        bounds[1],
                        bounds[2],
                        bounds[3],
                    )
                ]
            }
        )

    def get_part(part):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            return common.table_to_tile(
                tile, table=part_path, use_columns=use_columns, min_zoom=min_zoom
            )
        except ValueError:
            return None

    if num_parts > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parts) as pool:
            dfs = list(pool.map(get_part, range(num_parts)))
    else:
        # Don't bother creating a thread pool to do one thing
        dfs = [get_part(0)]

    dfs = [df for df in dfs if df is not None]

    if len(dfs) == 1:
        gdf = dfs[0]
    elif len(dfs):
        gdf = pd.concat(dfs)
    else:
        logging.warn("Failed to get any data")
        return None

    if point_convert is not None:
        gdf["geometry"] = gdf.geometry.centroid

    return gdf
