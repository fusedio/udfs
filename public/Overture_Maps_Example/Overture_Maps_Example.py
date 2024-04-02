@fused.udf
def udf(
    bbox: fused.types.TileGDF=None,
    release: str="2024-03-12-alpha-0",
    theme: str=None,
    type: str="building",
    use_columns: list=None,
    num_parts: int=None,
    min_zoom: int=None,
    polygon: str=None,
    point_convert: str=None
):
    import json
    import urllib.parse

    import pandas as pd
    import geopandas as gpd
    from shapely.geometry import shape, box
    import concurrent.futures

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f8f0c0f/public/common/"
    ).utils

    if num_parts is None:
        num_parts = 1 if type != "building" else 5

    if release == "2024-02-15-alpha-0":
        if type == "administrative_boundary":
            type = "administrativeBoundary"
        elif type == "land_use":
            type = "landUse"
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
    if not theme:
        theme = theme_per_type[type]

    if min_zoom is None:
        if theme == "admins":
            min_zoom = 7
        elif theme == "base":
            min_zoom = 9
        else:
            min_zoom = 12

    table_path = f"s3://us-west-2.opendata.source.coop/fused/overture/{release}/theme={theme}/type={type}"
    table_path = table_path.rstrip("/")

    if polygon is not None:
        decoded_string = urllib.parse.unquote(polygon)
        geom = json.loads(decoded_string)
        poly_gdf = gpd.GeoDataFrame({'geometry':[shape(geom['geometry'])]}, geometry="geometry", crs="EPSG:4326")
        bounds = poly_gdf.geometry.bounds
        print(bounds)
        bbox = gpd.GeoDataFrame({'geometry': [box(bounds.minx.loc[0], bounds.miny.loc[0], bounds.maxx.loc[0], bounds.maxy.loc[0])]})

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
        #print(df.columns)
        for col in df.columns:
            # Some overture columns do not serialize nicely and can have compatability
            # issues with some Parquet implementations.
            # Here we coerce to string to work around that.
            if col != "geometry":
                df[col] = df[col].apply(str)
            if point_convert is not None:
                gdf = gpd.GeoDataFrame(df)
                gdf['geometry'] = gdf.geometry.centroid
                df = gdf
        return df
    return None
