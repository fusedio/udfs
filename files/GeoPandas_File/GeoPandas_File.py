@fused.udf
def udf(path: str, preview: bool=False):
    import geopandas as gpd

    @fused.cache
    def get_geojson(path):
        # Caching is used to prevent doing many calls to the file system
        return gpd.read_file(path)

    gdf = get_geojson(path)
    if preview:
        return gdf.geometry
    return gdf
