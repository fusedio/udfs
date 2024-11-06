@fused.udf
def udf(bbox: fused.types.TileGDF = None, h3_size: int = None):
    import h3

    conn = fused.utils.common.duckdb_connect()

    # 1. Set H3 resolution
    x, y, z = bbox.iloc[0][["x", "y", "z"]]

    if not h3_size:
        res_offset = 1  # lower makes the hex finer
        h3_size = max(min(int(3 + bbox.z[0] / 1.5), 12) - res_offset, 2)

    print(h3_size)

    # 2. Load Overture Buildings
    gdf = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox, min_zoom=10)
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

    print(df)

    return df
