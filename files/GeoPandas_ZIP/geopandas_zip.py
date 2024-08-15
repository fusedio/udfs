@fused.udf
def udf(path: str, preview: bool):
    import geopandas as gpd

    # Add the path within the zip file here if not automatically detected
    path2 = "zip+${path}"
    gdf = gpd.read_file(path2)
    print(gdf)
    if preview:
        return gdf.geometry
    return gdf
