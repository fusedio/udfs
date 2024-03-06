@fused.udf
def udf(bbox,
        release="2024-02-15-alpha.0",
        theme="buildings",
        type=None,
        use_columns=None,
        num_parts=None,
        min_zoom=None,
):
    import json
    import pandas as pd
    import concurrent.futures
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils

    if use_columns:
        use_columns = json.loads(use_columns)
    
    if num_parts:
        # Override the num_parts detection logic (shouldn't be needed)
        num_parts = int(num_parts)
    else:
        num_parts = 1 if theme != "buildings" else 5
    
    if min_zoom:
        min_zoom = int(min_zoom)
    elif theme == "admins":
        min_zoom = 7
    elif theme == "base":
        min_zoom = 9
    else:
        min_zoom = 12

    default_type_per_theme = {
        "buildings": "building",
        "admins": "administrativeBoundary",
        "places": "place",
        "base": "landUse",
        "transportation": "segment",
    }
    if not type:
        type = default_type_per_theme[theme]
    
    table_path = f"s3://fused-asset/overture/{release}/theme={theme}/type={type}"
    table_path = table_path.rstrip("/")
    
    def get_part(part):
        part_path = f"{table_path}/part={part}/" if num_parts != 1 else table_path
        try:
            return utils.table_to_tile(bbox,
                                       table=part_path,
                                       use_columns=use_columns,
                                       min_zoom=min_zoom)
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
        print(df.columns)
        for col in df.columns:
            # Some overture columns do not serialize nicely and can have compatability
            # issues with some Parquet implementations.
            # Here we coerce to string to work around that.
            if col != "geometry":
                df[col] = df[col].apply(str)
        return df
    return None
