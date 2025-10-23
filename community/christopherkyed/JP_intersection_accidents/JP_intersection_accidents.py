@fused.udf
def udf(
    bounds: fused.types.TileGDF = None,
    min_zoom: int = 12,
    acc_year=2021
):
    import geopandas as gpd
    import duckdb

    if bounds.z[0] >= min_zoom:
        # --------------------------------------------------------------------
        # Load Overture data (roads/connector) – keep in EPSG:4326 (no reproj)
        # --------------------------------------------------------------------
        gdf = get_overture(
            bbox=bounds,
            release="2025-05-21-0",
            theme='transportation',
            overture_type='connector',
            use_columns=['id'],
            num_parts=None,
            min_zoom=min_zoom,
            polygon=None,
            point_convert=None
        )

        # Extract lat/lon directly from point geometry (already in 4326)
        gdf["lat"] = gdf.geometry.y
        gdf["lon"] = gdf.geometry.x
        
        # Keep only necessary columns for DuckDB
        int_df = gdf[["id", "lat", "lon"]].copy()

        # --------------------------------------------------------------------
        # Load accidents (HTML UDF) – assumed to contain latitude/longitude cols
        # --------------------------------------------------------------------
        df_acc = fused.run('html_accident_h3_tile', bounds=bounds, year=acc_year)
        
        # --------------------------------------------------------------------
        # DuckDB setup
        # --------------------------------------------------------------------
        con = duckdb.connect()
        con.sql("INSTALL spatial; LOAD spatial;")
        con.sql("INSTALL h3 FROM community; LOAD h3;")

        # Register tables
        con.register("intersections", int_df)    # columns: id, lat, lon
        con.register("acc", df_acc)        # expect columns: latitude, longitude (or lat, lon)

        # --------------------------------------------------------------------
        # Build points with H3 hex and its 2-ring neighbours, then join with accidents
        # --------------------------------------------------------------------
        sql = """
            WITH points AS (
                SELECT
                    id,
                    h3_latlng_to_cell(lat, lon, 12) AS hex,
                    h3_grid_disk(h3_latlng_to_cell(lat, lon, 12), 2) AS hex_neighbour
                FROM intersections
            ),
            accidents AS (
                SELECT
                prefecture_name,
                year,
                month,
                day,
                hour,
                age_A,
                age_B,
                injuries,
                hex
            FROM acc
            )
            SELECT
                p.id,
                p.hex,
                MODE(prefecture_name) as prefecture_name,
                COUNT(a.*) as acc_cnt,
                (AVG(a.age_A) + AVG(a.age_B))/2 AS avg_age,
                SUM(a.injuries) as sum_injuries
            FROM points AS p
            INNER JOIN accidents AS a
            ON a.hex = ANY(p.hex_neighbour)
            GROUP BY p.id, p.hex
            ORDER BY p.id;
        """
        df_joined = con.sql(sql).df()
        print(df_joined)

        df_joined = df_joined[df_joined['acc_cnt'] > 0]

        return df_joined


# Load Fused helper functions
table_to_tile = fused.load("https://github.com/fusedio/udfs/tree/3569595/public/common/").utils.table_to_tile
duckdb_connect = fused.load("https://github.com/fusedio/udfs/tree/3569595/public/common/").utils.duckdb_connect

def get_overture(
    bbox: fused.types.TileGDF = None,
    release: str = "2024-08-20-0",
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

    # Load Fused helper functions
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f8f0c0f/public/common/"
    ).utils

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
    print(table_path)
    if polygon is not None:
        polygon=gpd.from_features(json.loads(polygon))
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
            return table_to_tile(
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
        gdf = pd.concat(dfs)

    else:
        logging.warn("Failed to get any data")
        return None

    if point_convert is not None:
        gdf["geometry"] = gdf.geometry.centroid

    return gdf

    