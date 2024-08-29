@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    index_min: float = 0.1,
    index_max: float = 1.0,
    index_method: int = 0,
    return_mask: bool = True,
):

    from utils import process_image, url_to_arr

    # Get the bounding box coordinates
    x, y, z = bbox[['x', 'y', 'z']].iloc[0]
    print('Zoom level:', z)
    # ArcGIS Online World Imagery basemap
    image_path = f'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
    if not return_mask:
        return url_to_arr(image_path)

    # Processing
    available_indices = [
        'VARI',  # Visible Atmospherically Resistant Index
        'GLI',  # Green Leaf Index
        'RGRI',  # Red-Green Ratio Index
    ]
    vegetation_index = available_indices[index_method]

    vegetation_mask = process_image(image_path, z, index_min, index_max,
                                    vegetation_index)

    mask = (vegetation_mask * 255).astype('uint8')
    import numpy as np
    return np.stack([mask * 1, mask * 0, mask * 1, mask // 2])
