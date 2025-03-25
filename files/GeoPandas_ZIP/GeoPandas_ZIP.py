@fused.udf
def udf(path: str, preview: bool=False):
    import geopandas as gpd

    # Add the path within the zip file here if not automatically detected
    # path = f"zip+{path}"
    gdf = gpd.read_file(path)
    print(gdf)
    if preview:
        return gdf.geometry
    return gdf
