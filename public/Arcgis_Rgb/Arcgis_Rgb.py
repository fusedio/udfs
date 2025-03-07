@fused.udf
def udf(bounds: fused.types.TileGDF = None):
    # Get the bounding box coordinates
    x, y, z = bounds[["x", "y", "z"]].iloc[0]
    # Load pinned versions of utility functions.
    utils = fused.load("https://github.com/fusedio/udfs/tree/ee9bec5/public/common/").utils
    # ArcGIS Online World Imagery basemap
    return utils.url_to_arr(f"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}")
