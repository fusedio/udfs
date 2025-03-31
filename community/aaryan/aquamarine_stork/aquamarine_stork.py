@fused.udf
def udf(bounds: fused.types.Bounds = None, n: int = 10):
    """
    Change n value to see more or less rotations
    Or edit any of the code directly to see changes live!
    """
    import geopandas as gpd
    import shapely

    common = fused.load("https://github.com/fusedio/udfs/tree/2bfc3e0/public/common/").utils

    if bounds is not None:
        gdf = common.bounds_to_gdf(bounds)
        geometry = gdf.geometry.iloc[0]
    else:
        geometry = shapely.box(-122.549, 37.681, -122.341, 37.818)

    geoms = [shapely.affinity.rotate(geometry, 3 * i) for i in range(n)]
    return gpd.GeoDataFrame({"value": list(range(n))}, geometry=geoms)
