@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    class_source: str = 'combined', 
    building_source: str = 'Overture'
):
    import duckdb
    import geopandas as gpd

    # Load pinned versions of utility functions.
    overture_utils = fused.load("https://github.com/fusedio/udfs/tree/2f138dd/public/Overture_Maps_Example/").utils
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/2f138dd/public/common/").utils


    conn = common_utils.duckdb_connect()

    if class_source=='Overture':
        metric = 'subtype'
    elif class_source=='ORNL':
        metric = 'OCC_CLS'
    else: 
        metric = 'combined_source'

    # 1. Load Overture Buildings
    gdf_overture = overture_utils.get_overture(bounds=bounds)

    # 2. Load Oak Ridge Buildings
    gdf_oakridge = common_utils.table_to_tile(
        bounds,
        table="s3://fused-asset/infra/building_oak/",
        use_columns=None,
        min_zoom=11,
    )

    # 3. Enrich
    if building_source == "Overture":
        joined = gdf_overture.sjoin(gdf_oakridge)
    elif building_source == "ORNL":
        joined = gdf_oakridge.sjoin(gdf_overture)
    
    joined["combined_source"] = joined.apply(
        lambda x: x.get("subtype") or x.get("OCC_CLS").lower(), axis=1
    )
    joined["combined_source"] = joined["combined_source"].apply(
        lambda x: "government" if x == "civic" else x
    )

    # 4. Print stats with DuckDB
    joined_str = joined.copy()
    joined_str["geometry"] = joined_str["geometry"].astype(str)
    df = conn.sql(
        """
        SELECT combined_source, COUNT(*) cnt
        FROM joined_str
        GROUP BY combined_source
        ORDER BY 2 DESC"""
    ).df()
    print(df)

    # 5. Decide column to render
    if metric == "OCC_CLS":
        joined["OCC_CLS"] = joined["OCC_CLS"].str.lower()

    joined["metric"] = joined[metric]

    return joined
