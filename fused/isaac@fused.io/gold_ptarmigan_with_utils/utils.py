def my_util():
    print("hello world")

def the_udf(bbox=None, n=10):
    import geopandas as gpd
    import shapely

    default_bbox = shapely.box(-122.549, 37.681, -122.341, 37.818)
    tile_bbox = bbox.geometry[0] if bbox is not None else default_bbox
    geoms = [shapely.affinity.rotate(tile_bbox, 3 * i) for i in range(n)]

    return gpd.GeoDataFrame({
      'n': list(range(n)),
      'r': [i * 30 for i in range(n)],
      'g': [i * 30 for i in range(n)],
      'b': [255] * n,
    }, geometry=geoms)

