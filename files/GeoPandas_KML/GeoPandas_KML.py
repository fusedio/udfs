@fused.udf
def udf(path: str, preview: bool=False):
    import fiona
    import geopandas as gpd

    fiona.drvsupport.supported_drivers["KML"] = "rw"
    gdf = gpd.read_file(path)
    print(gdf)
    gdf = gdf.to_crs(4326)
    if preview:
        return gdf.geometry
    return gdf
