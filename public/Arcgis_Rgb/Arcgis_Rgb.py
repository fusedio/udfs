@fused.udf
def udf(bbox: fused.types.TileGDF = None):
    # Get the bounding box coordinates
    x, y, z = bbox[["x", "y", "z"]].iloc[0]
    # ArcGIS Online World Imagery basemap
    return fused.utils.common.url_to_arr(f"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}")
