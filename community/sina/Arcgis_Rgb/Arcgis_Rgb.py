@fused.udf
def udf(bounds: fused.types.Bounds = [5.494,46.050,15.443,56.132]):
    # Load pinned versions of utility functions.
    common = fused.load(
    "https://github.com/fusedio/udfs/tree/2f41ae1/public/common/"
    ).utils
    bounds = common.get_tiles(bounds, clip=True)
    # Get the bounding box coordinates
    x, y, z = bounds[["x", "y", "z"]].iloc[0]
    # ArcGIS Online World Imagery basemap
    return utils.url_to_arr(f"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}")
