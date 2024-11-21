@fused.udf
def udf(
    bbox: fused.types.TileGDF = None, 
    resolution: int = 10,
    min_count: int = 30
):
    """
    Join H3 Cab Pickup Data With Overture Buildings in NYC.
    In the Visualize tab, set extruded to true to see the scale.
     """

    
    @fused.cache
    def get_cabs(resolution, min_count):
        gdf = fused.run(
            "UDF_DuckDB_H3_Example", 
            resolution=resolution, 
            min_count=min_count
        )
        return gdf
    # So we can visualize by the count (cnt) attribute with the cell boundary.
    gdf_cab = get_cabs(resolution, min_count)
    
    # Load Overture Buildings.
    gdf_overture = fused.run(
        "UDF_Overture_Maps_Example",
        theme="buildings",
        overture_type="building",
        bbox=bbox,
        min_zoom=10
    )
    # Join the two dataframes.
    gdf_joined = gdf_overture.sjoin(gdf_cab)
    
    print(gdf_joined)
    return gdf_joined