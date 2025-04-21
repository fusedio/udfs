
@fused.cache
def load_owlvit_model(model_name="google/owlvit-base-patch32"):
    """Load and cache the OWL-ViT model"""
    from transformers import OwlViTProcessor, OwlViTForObjectDetection
    import os
    import torch
    
    cache_dir = "/mount/tmp/owlvit/"
    os.makedirs(cache_dir, exist_ok=True)
    
    # Set device
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    processor = OwlViTProcessor.from_pretrained(model_name, cache_dir=cache_dir)
    model = OwlViTForObjectDetection.from_pretrained(model_name, cache_dir=cache_dir)
    model = model.to(device)
    
    return processor, model

@fused.cache
def detect_objects(image_array, text_queries, processor, model, threshold=0.1, return_debug_info=False):
    """Perform object detection on an image using OWL-ViT with optional debug info"""
    import torch
    from PIL import Image
    import numpy as np

    if isinstance(image_array, np.ndarray):
        #do check if the image has 3 bands or not 
        image_array = image_array.transpose(1, 2, 0)

        if image_array.dtype != np.uint8:
            min_val = np.min(image_array)
            max_val = np.max(image_array)
            if max_val > min_val:
                image_array = ((image_array - min_val) / (max_val - min_val) * 255).astype(np.uint8)
            else:
                image_array = np.zeros_like(image_array, dtype=np.uint8)

        image = Image.fromarray(image_array)
    else:
        image = image_array
    
    if not text_queries:
        print("Please add text queries")
    

    model_text_format = [text_queries]
    
    inputs = processor(text=model_text_format, images=image, return_tensors="pt")
    
    device = next(model.parameters()).device
    inputs = {k: v.to(device) for k, v in inputs.items()}

    with torch.no_grad():
        outputs = model(**inputs)

    target_sizes = torch.tensor([(image.height, image.width)]).to(device)

    raw_threshold = 0.0001 if return_debug_info else threshold
    results = processor.post_process_object_detection(
        outputs=outputs, 
        target_sizes=target_sizes, 
        threshold=raw_threshold
    )
    
    # Extract boxes, scores and labels from the first image
    result = results[0]
    boxes = result["boxes"].cpu().numpy()
    scores = result["scores"].cpu().numpy()
    labels = result["labels"].cpu().numpy()
    
    # Format results
    all_detections = []
    for box, score, label_idx in zip(boxes, scores, labels):
        # Map the label index to the corresponding text query
        # Handle potential index errors
        try:
            label = text_queries[label_idx]
        except IndexError:
            # Fallback if index is out of range
            label = f"object_{label_idx}"
        
        # Add to results if above threshold (for final results)
        if score >= threshold:
            all_detections.append({
                "box": box.tolist(),  # [xmin, ymin, xmax, ymax]
                "score": float(score),
                "label": label
            })
    
    if return_debug_info:
        # Return all raw scores for debugging
        return {
            "detections": all_detections,
            "raw_scores": scores.tolist(),
            "raw_labels": [text_queries[l] if l < len(text_queries) else f"object_{l}" for l in labels]
        }
    
    return all_detections


def filter_size(detections, max_box_size, min_box_size):
    filtered_detections = []
    for detection in detections:
        box_pixels = detection["box"]
        xmin_px, ymin_px, xmax_px, ymax_px = box_pixels

        width_px = xmax_px - xmin_px
        height_px = ymax_px - ymin_px

        if (min_box_size <= width_px <= max_box_size and 
            min_box_size <= height_px <= max_box_size):
            filtered_detections.append(detection)

    print(f"Filtered from {len(detections)} to {len(filtered_detections)} objects based on size")
    return filtered_detections


def process_image_chip(bounds, tif_path, processor, model, text_queries, threshold, chip_size, min_box_size, max_box_size):
    import fused
    utils = fused.load("https://github.com/fusedio/udfs/tree/5432edc/public/common/").utils

    tiles = utils.get_tiles(bounds)
    image_data = utils.read_tiff(tiles, tif_path, output_shape=(chip_size, chip_size))

    if image_data is None:
        return [], None, None

    detections = detect_objects(image_data, text_queries, processor, model, threshold)
    filtered_detections = filter_size(detections, max_box_size, min_box_size)

    return filtered_detections, tiles, image_data

def convert_detections_to_geometries(detections, tile_bounds, image_shape):
    from shapely.geometry import box
    geometries = []
    properties = []

    img_height, img_width = image_shape

    x_scale = (tile_bounds[2] - tile_bounds[0]) / img_width
    y_scale = (tile_bounds[3] - tile_bounds[1]) / img_height

    for detection in detections:
        xmin_px, ymin_px, xmax_px, ymax_px = detection["box"]
        xmin_geo = tile_bounds[0] + xmin_px * x_scale
        ymin_geo = tile_bounds[3] - ymax_px * y_scale 
        xmax_geo = tile_bounds[0] + xmax_px * x_scale
        ymax_geo = tile_bounds[3] - ymin_px * y_scale

        geometry = box(xmin_geo, ymin_geo, xmax_geo, ymax_geo)
        geometries.append(geometry)
        properties.append({
            "score": detection["score"],
            "label": detection["label"]
        })

    return geometries, properties

