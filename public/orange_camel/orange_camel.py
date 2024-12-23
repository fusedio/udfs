@fused.udf
def udf(bbox: fused.types.TileGDF=None, n: int=10):
    import geopandas as gpd
    import shapely

    default_bbox = shapely.box(-122.549, 37.681, -122.341, 37.818)
    tile_bbox = bbox.geometry.iloc[0] if bbox is not None else default_bbox
    geoms = [shapely.affinity.rotate(tile_bbox, 3 * i) for i in range(n)]

    return gpd.GeoDataFrame({"value": list(range(n))}, geometry=geoms)