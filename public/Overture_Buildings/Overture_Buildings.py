@fused.udf
def udf_overture(
    bbox,
    release="2024-02-15-alpha.0",
    theme="buildings",
    type=None,
    use_columns=None,
    num_parts=None,
    min_zoom=None,
):
    import concurrent.futures
    import json

    import geopandas as gpd
    import pandas as pd

    # Load utility functions
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f8f0c0f/public/common/"
    ).utils

    if isinstance(bbox, str):
        bbox = gpd.GeoDataFrame.from_features(json.loads(bbox))

    # Set defaults acording to zoom level (to avoid fetching too much data)
    if min_zoom:
        min_zoom = int(min_zoom)
    elif theme == "admins":
        min_zoom = 7
    elif theme == "base":
        min_zoom = 9
    else:
        min_zoom = 12

    # Parameters for the overture table
    default_type_per_theme = {
        "buildings": "building",
        "admins": "administrativeBoundary",
        "places": "place",
        "base": "landUse",
        "transportation": "segment",
    }
    if not type:
        type = default_type_per_theme[theme]

    # Remote table with partitioned data
    table_path = f"s3://fused-asset/overture/{release}/theme={theme}/type={type}"
    table_path = table_path.rstrip("/")

    # Partitions
    num_parts = 1 if theme != "buildings" else 5

    # Get data from each partition
    def get_part(part):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            return utils.table_to_tile(
                bbox, table=part_path, use_columns=use_columns, min_zoom=min_zoom
            )
        except ValueError:
            return None

    # Use thread pool if multipart
    if num_parts > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_parts) as pool:
            dfs = list(pool.map(get_part, range(num_parts)))
    else:
        dfs = [get_part(0)]
    dfs = [df for df in dfs if df is not None]
    if len(dfs):
        df = pd.concat(dfs)
        print(df.columns)
        for col in df.columns:
            # Some overture columns do not serialize nicely and can have compatability
            # issues with some Parquet implementations.
            # Here we coerce to string to work around that.
            if col != "geometry":
                df[col] = df[col].apply(str)
        return df
    else:
        print("No data found.")
        return None
