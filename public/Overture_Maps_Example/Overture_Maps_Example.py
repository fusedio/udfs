import geopandas as gpd


@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-03-12-alpha-0",
    theme: str = None,
    osm_type: str = None,
    use_columns: list = None,
    num_parts: int = None,
    min_zoom: int = None,
    polygon: gpd.GeoDataFrame = None,
    point_convert: str = None,
):
    import logging
    import concurrent.futures

    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import shape, box

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f8f0c0f/public/common/"
    ).utils

    if release == "2024-02-15-alpha-0":
        if osm_type == "administrative_boundary":
            osm_type = "administrativeBoundary"
        elif osm_type == "land_use":
            osm_type = "landUse"
        theme_per_type = {
            "building": "buildings",
            "administrativeBoundary": "admins",
            "place": "places",
            "landUse": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }
    else:
        theme_per_type = {
            "building": "buildings",
            "administrative_boundary": "admins",
            "place": "places",
            "land_use": "base",
            "water": "base",
            "segment": "transportation",
            "connector": "transportation",
        }

    if theme is None:
        theme = theme_per_type.get(osm_type, "buildings")

    if osm_type is None:
        type_per_theme = {v: k for k, v in theme_per_type.items()}
        osm_type = type_per_theme[theme]

    if num_parts is None:
        num_parts = 1 if osm_type != "building" else 5

    if min_zoom is None:
        if theme == "admins":
            min_zoom = 7
        elif theme == "base":
            min_zoom = 9
        else:
            min_zoom = 12

    table_path = f"s3://us-west-2.opendata.source.coop/fused/overture/{release}/theme={theme}/type={osm_type}"
    table_path = table_path.rstrip("/")

    if polygon is not None:
        bounds = polygon.geometry.bounds
        bbox = gpd.GeoDataFrame(
            {
                "geometry": [
                    box(
                        bounds.minx.loc[0],
                        bounds.miny.loc[0],
                        bounds.maxx.loc[0],
                        bounds.maxy.loc[0],
                    )
                ]
            }
        )

    def get_part(part):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            return utils.table_to_tile(
                bbox, table=part_path, use_columns=use_columns, min_zoom=min_zoom
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

    if len(dfs):
        df = pd.concat(dfs)
        # Some overture columns do not serialize nicely and can have compatability
        # issues with some Parquet implementations.
        # Here we coerce to string to work around that.

        # Note we also assume that there can't be an alternate value if there is no main value
        if "categories" in df.columns:
            df["categories"] = [
                {"main": [c["main"]], "alternate": c.get("alternate", -1).tolist()}
                if c is not None and c["alternate"] is not None
                else {"main": [c["main"]], "alternate": []}
                if c is not None
                else {"main": [], "alternate": []}
                for c in df["categories"]
            ]
        extra_cols = ["geometry", "categories"]
        gdf = gpd.GeoDataFrame(
            pd.concat(
                [
                    df[[c for c in df.columns if c not in extra_cols]].astype(str),
                    df[[extra for extra in extra_cols if extra in df.columns]],
                ],
                axis=1,
            )
        )

    else:
        logging.warn("Failed to get any data")
        return None

    if point_convert is not None:
        gdf["geometry"] = gdf.geometry.centroid

    return gdf
