@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    metric: str = "combined_source"  # Combined
    # metric: str = 'subtype' # Overture
    # metric: str = 'OCC_CLS'  # Oak Ridge
):
    import duckdb
    import geopandas as gpd

    conn = fused.utils.common.duckdb_connect()

    # 1. Load Overture Buildings
    gdf_overture = fused.utils.Overture_Maps_Example.get_overture(bbox=bbox)

    # 2. Load Oak Ridge Buildings
    gdf_oakridge = fused.utils.common.table_to_tile(
        bbox,
        table="s3://fused-asset/infra/building_oak/",
        use_columns=None,
        min_zoom=11,
    )

    # 3. Enrich
    gdf_overture = gdf_overture.sjoin(gdf_oakridge)
    gdf_overture["combined_source"] = gdf_overture.apply(
        lambda x: x.get("subtype") or x.get("OCC_CLS").lower(), axis=1
    )
    gdf_overture["combined_source"] = gdf_overture["combined_source"].apply(
        lambda x: "government" if x == "civic" else x
    )

    # 4. Print stats with DuckDB
    gdf_overture_str = gdf_overture.copy()
    gdf_overture_str["geometry"] = gdf_overture_str["geometry"].astype(str)
    df = conn.sql(
        """
        SELECT combined_source, COUNT(*) cnt
        FROM gdf_overture_str
        GROUP BY combined_source"""
    ).df()
    print(df)

    # 5. Decide column to render
    if metric == "OCC_CLS":
        gdf_overture["OCC_CLS"] = gdf_overture["OCC_CLS"].str.lower()

    gdf_overture["metric"] = gdf_overture[metric]

    return gdf_overture
