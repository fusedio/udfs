@fused.udf
def udf(
    url="s3://fused-users/fused/census/tiger/tl_rd22_11_bg.zip",
):
    import geopandas as gpd

    gdf = gpd.read_file(url)
    return gdf
