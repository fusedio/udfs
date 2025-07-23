import geopandas as gpd

@fused.udf
def udf_rgb_tiles(tile: gpd.GeoDataFrame):
    utils = fused.load('https://github.com/fusedio/udfs/tree/004b8d9/public/common/').utils
    x, y, z = tile[["x", "y", "z"]].iloc[0]
    return utils.url_to_arr(f"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}")

@fused.udf
def udf(    
    bounds: fused.types.Bounds=[-110.834,32.152,-110.833,32.153],
    chip_len=256,
    buffer_degree=0.00001,
    # weights_path = "s3://fused-asset/public_udf_data/dl4eo/best.onnx",
    # Using URI to make it simpler to download files when logged out
    weights_path = "https://fused-asset.s3.amazonaws.com/public_udf_data/dl4eo/best.onnx",
    return_predictions: bool = True
):
    
    import geopandas as gpd
    import shapely
    import rasterio
    import numpy as np

    # convert bounds to tile
    common_utils = fused.load("https://github.com/fusedio/udfs/tree/bb712a5/public/common/").utils
    zoom = common_utils.estimate_zoom(bounds)
    tile = common_utils.get_tiles(bounds, zoom=zoom)


    # Load imagery
    tile.geometry = tile.buffer(buffer_degree).geometry
    arr = fused.run(udf_rgb_tiles, tile=tile).astype(np.uint8)

    if not return_predictions:
        # Display the actual image used for prediction
        return arr
    
    # Predict
    try:
        boxes = predict(arr, weights_path=weights_path)
    except FileNotFoundError as e:
        raise AssertionError("It seems like weights are missing to run this...", e)
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


@fused.cache
def predict(
    arr,
    weights_path,
    threshold=0.5,
):
    import os
    import time
    import torch
    import ultralytics
    import numpy as np

    if arr is None:
        return None
        
    img = arr.transpose(1, 2, 0)[:,:,:3]

    # check if GPU if available
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    # Set weights file
    mount_weights_path = fused.file_path("mount_best.onnx")

    try:
        if not os.path.exists(mount_weights_path):
            path = fused.download(weights_path, "mount_best.onnx")
            print(f"Downloaded: {path}")
    except Exception as e:
        print("Error", e)
        return 
    
    # Load a model
    start_time = time.time()
    model = ultralytics.YOLO(weights_path, task='detect')  # previously trained YOLOv8n model
    end_time = time.time()
    print(f"Model loaded in: {round(end_time-start_time, 3)} sec.")

    if not isinstance(img, np.ndarray) or len(img.shape) != 3 or img.shape[2] != 3:
        raise BaseException("predit_image(): input 'img' shoud be single RGB image in PIL or Numpy array format.")

    width, height = img.shape[0], img.shape[1]
    
    # Predict
    start_time = time.time()
    results = model.predict([img], imgsz=(width, height), conf=threshold)
    end_time = time.time()
    print(f"Inference took: {round(end_time-start_time, 3)} sec.")
    boxes = results[0].boxes
    boxes = [box.xyxy.cpu().squeeze().int().tolist() for box in boxes]
    print(f"Nb aircrafts in Tile: {len(boxes)}")
    
    return boxes

