@fused.udf
def udf(path: str, preview: bool=False, layer="tracks"):
    import fiona
    import geopandas as gpd

    gdf = gpd.read_file(path, layer=layer)
    print(gdf)
    gdf = gdf.to_crs(4326)
    if preview:
        return gdf.geometry
    return gdf
