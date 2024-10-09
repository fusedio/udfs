@fused.udf
def udf(
    url="https://www2.census.gov/geo/tiger/TIGER_RD18/STATE/11_DISTRICT_OF_COLUMBIA/11/tl_rd22_11_bg.zip",
):
    import geopandas as gpd

    gdf = gpd.read_file(url)
    return gdf
