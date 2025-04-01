import geopandas as gpd

@fused.udf
def udf_rgb_tiles(tile: gpd.GeoDataFrame):
    utils = fused.load('https://github.com/fusedio/udfs/tree/004b8d9/public/common/').utils
    x, y, z = tile[["x", "y", "z"]].iloc[0]
    return utils.url_to_arr(f"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}")

@fused.udf
def udf(    
    bounds: fused.types.Bounds=None,
    chip_len=256,
    buffer_degree=0.00001,
    weights_path = "s3://fused-asset/misc/dl4eo/best.onnx"
):
    import geopandas as gpd
    import shapely
    import rasterio
    from utils import predict
    import numpy as np

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)


    # Load imagery
    tile.geometry = tile.buffer(buffer_degree).geometry
    arr = fused.run(udf_rgb_tiles, tile=tile).astype(np.uint8)
    
    # Predict
    try:
        boxes = predict(arr, weights_path=weights_path)
    except FileNotFoundError as e:
        raise AssertionError("It seems like weights are missing to run this...")
    print(boxes)
    if boxes is None:
        print("Warning: Unable to run inference...")
        return None

    # Transform prediction bounding boxes from px to degrees
    tile.set_crs(4326, inplace=True)
    transform = rasterio.transform.from_bounds(*bounds, chip_len, chip_len)
    
    tf_boxes = []
    for box in boxes:
        xmin, ymin, xmax, ymax = box
        margin = 10
        xc = (xmin + xmax) / 2.0
        yc = (ymin + ymin) / 2.0
        width = height = 2500
        
        # Filter out boxes outside of margins
        if xc < margin or xc > width - margin or yc < margin or yc > height - margin:
            continue
 
        xmin_new, ymin_new = transform * (xmin, ymin)
        xmax_new, ymax_new = transform * (xmax, ymax)
        tf_boxes.append([xmin_new, ymin_new, xmax_new, ymax_new])

    return gpd.GeoDataFrame(geometry=[shapely.box(xmin, ymin, xmax, ymax) for xmin, ymin, xmax, ymax in tf_boxes])
