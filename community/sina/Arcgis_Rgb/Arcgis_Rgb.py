@fused.udf
def udf(bounds: fused.types.Bounds = [5.494,46.050,15.443,56.132]):
    # Load pinned versions of utility functions.
    common = fused.load("https://github.com/fusedio/udfs/tree/364f5dd/public/common/")
    bounds = common.get_tiles(bounds, clip=True)
    # Get the bounding box coordinates
    x, y, z = bounds[["x", "y", "z"]].iloc[0]
    # ArcGIS Online World Imagery basemap
    return common.url_to_arr(f"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}")
