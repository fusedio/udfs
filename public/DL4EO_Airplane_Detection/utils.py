@fused.cache
def predict(
    arr,
    threshold=0.5,
    weights_path = "s3://fused-users/fused/asset/dl4eo/best.onnx"
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
    WEIGHTS_FILE = "/mnt/cache/best.onnx"
    
    if not os.path.exists(WEIGHTS_FILE):
        path = fused.download(weights_path, 'best.onnx')
        print(f"Downloaded: {path}")
    
    # Load a model
    start_time = time.time()
    model = ultralytics.YOLO('/mnt/cache/yolov8s.pt', task='detect')  # load an official model
    model = ultralytics.YOLO(WEIGHTS_FILE, task='detect')  # previously trained YOLOv8n model
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
    print(f"Aircrafts in Tile: {len(boxes)}.")
    
    return boxes

