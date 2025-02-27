@fused.udf
def udf(bounds: fused.types.Tile = None, h3_size: int = None, h3_scale: int=2):
    import h3

    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/Overture_Maps_Example/").utils

    conn = utils.duckdb_connect()

    # 1. Set H3 resolution
    x, y, z = bounds.iloc[0][["x", "y", "z"]]

    if not h3_size:
        h3_size = max(min(int(3 + bounds.z[0] / 1.5), 12) - h3_scale, 2)

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
