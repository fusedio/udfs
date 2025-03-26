@fused.udf
def udf(path: str, preview: bool=False):
    import geopandas as gpd

    gdf = gpd.read_file(path)
    print(gdf)
    if preview:
        return gdf.geometry
    return gdf
