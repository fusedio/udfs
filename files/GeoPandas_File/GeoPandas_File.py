@fused.udf
def udf(path: str, preview: bool):
    import geopandas as gpd
    import fsspec

    extension = path.rsplit(".", maxsplit=1)[-1]
    driver = (
        "GPKG"
        if extension == "gpkg"
        else ("ESRI Shapefile" if extension == "shp" else "GeoJSON")
    )

    with fsspec.open(path) as f:
        gdf = gpd.read_file(path, driver=driver)
    print(gdf)
    if preview:
        return gdf.geometry
    return gdf
