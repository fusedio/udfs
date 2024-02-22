@fused.udf
def udf(bbox=None, n=10):
    import geopandas as gpd
    import shapely

    default_bbox = shapely.box(-122.549, 37.681, -122.341, 37.818)
    tile_bbox = bbox.geometry[0] if bbox is not None else default_bbox
    geoms = [shapely.affinity.rotate(tile_bbox, 3 * i) for i in range(n)]

    df = gpd.GeoDataFrame({
      'n': list(range(n)),
      'r': [255] * n,
      'g': [i * 30 for i in range(n)],
      'b': [255] * n,
    }, geometry=geoms)
    df['name']='Jim'
    return df