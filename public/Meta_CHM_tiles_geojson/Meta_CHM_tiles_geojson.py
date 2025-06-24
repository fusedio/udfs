@fused.udf
def udf(
    bounds: fused.types.Bounds = None,
    subdivide_meta_tile_factor: int = 10,
    use_centroid_method: bool = True,
    path: str = "https://dataforgood-fb-data.s3.amazonaws.com/forests/v1/alsgedi_global_v6_float/tiles.geojson",
):
    import geopandas as gpd
    from shapely.geometry import Point
    from shapely.geometry import box

    @fused.cache
    def get_geojson(path):
        return gpd.read_file(path)

    gdf = get_geojson(path)

    if bounds is None:
        print(f"returning all tiles")
        return gdf

    if use_centroid_method:
        centroid_x = (bounds[0] + bounds[2]) / 2
        centroid_y = (bounds[1] + bounds[3]) / 2
        matches = gdf[gdf.contains(Point(centroid_x, centroid_y))]
    
    else:
        # Clipping to current bounds extent
        bounds_geom = box(*bounds)
        matches = gdf[gdf.geometry.intersects(bounds_geom)]

    print(f"{matches.shape=}")
    return matches