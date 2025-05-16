@fused.udf
def udf(bounds: fused.types.Bounds = [-122.437,37.772,-122.404,37.799], h3_size: int = None, h3_scale: int=2):
    import h3

    # Load pinned versions of utility functions.
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/5432edc/public/Overture_Maps_Example/").utils
    utils = fused.load("https://github.com/fusedio/udfs/tree/5432edc/public/common/").utils

    # convert bounds to tile
    tile = utils.get_tiles(bounds)


    conn = utils.duckdb_connect()

    # 1. Set H3 resolution
    x, y, z = tile.iloc[0][["x", "y", "z"]]

    if not h3_size:
        h3_size = max(min(int(3 + z / 1.5), 12) - h3_scale, 2)

    print(f"H3 Resolution: {h3_size}")

    # 2. Load Overture Buildings
    gdf = overture_utils.get_overture(bounds=bounds, min_zoom=10)
    if len(gdf) < 1:
        return

    # 3. Derive metrics
    gdf["area_m2"] = gdf.to_crs(gdf.estimate_utm_crs()).area.round(2)
    gdf["perimeter_m"] = gdf.to_crs(gdf.estimate_utm_crs()).length.round(2)
    gdf["centroid"] = gdf.geometry.centroid

    # 4. Map each building to an H3 cell
    gdf["hex"] = gdf["centroid"].apply(lambda x: h3.latlng_to_cell(x.y, x.x, h3_size))

    # 5. Group by H3 with DuckDB
    df = gdf[["area_m2", "perimeter_m", "hex", "height"]]
    df = conn.sql(
        """
        SELECT
            hex,
            avg(area_m2) avg_area,
            avg(perimeter_m) avg_perimeter,
            count(*) cnt,
            avg(height) avg_height
        FROM df
            GROUP BY hex"""
    ).df()

    # print(df)

    return df
