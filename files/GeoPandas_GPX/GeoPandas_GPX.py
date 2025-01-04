@fused.udf
def udf(path: str, preview: bool):
    import fiona
    import geopandas as gpd

    gdf = gpd.read_file(path, layer="tracks")
    print(gdf)
    gdf = gdf.to_crs(4326)
    if preview:
        return gdf.geometry
    return gdf
