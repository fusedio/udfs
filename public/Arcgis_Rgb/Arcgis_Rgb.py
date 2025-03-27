@fused.udf
def udf(bounds: fused.types.Bounds = None):
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = utils.estimate_zoom(bounds)
    bounds = utils.get_tiles(bounds, zoom=zoom)
    # Get the bounding box coordinates
    x, y, z = bounds[["x", "y", "z"]].iloc[0]
    # ArcGIS Online World Imagery basemap
    return utils.url_to_arr(f"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}")
