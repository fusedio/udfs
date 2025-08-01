@fused.udf
def udf(
    bounds: fused.types.Bounds = [-122.841,49.290,-122.835,49.292],
    index_min: float = 0.3,
    index_max: float = 1.0,
    index_method: int = 0,
    return_mask: bool = True,
):
    import numpy as np

    # convert bounds to tile
    common = fused.load("https://github.com/fusedio/udfs/tree/b7637ee/public/common/")
    tile = common.get_tiles(bounds, target_num_tiles=1)
    
    # Get the bounding box coordinates
    x, y, z = tile[["x", "y", "z"]].iloc[0]
    print("Zoom level:", z)

    # ArcGIS Online World Imagery basemap
    image_path = f"https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
    
    if return_mask:
        # Processing
        available_indices = [
            "VARI",  # Visible Atmospherically Resistant Index
            "GLI",  # Green Leaf Index
            "RGRI",  # Red-Green Ratio Index
        ]
        vegetation_index = available_indices[index_method]
    
        vegetation_mask = process_image(
            image_path, z, index_min, index_max, vegetation_index
        )
        mask = (vegetation_mask * 255).astype("uint8")
    
        return np.stack([mask * 0, mask * 1, mask * 0, mask // 2]), list(tile.total_bounds)
    else:
        return url_to_arr(image_path), list(tile.total_bounds)

@fused.cache
def url_to_arr(url, return_colormap=False):
    from io import BytesIO
    import rasterio
    import requests
    response = requests.get(url)
    print(response.status_code)
    with rasterio.open(BytesIO(response.content)) as dataset:
        if return_colormap:
            colormap = dataset.colormap
            return dataset.read(), dataset.colormap(1)
        else:
            return dataset.read()


@fused.cache
def get_bands(image_path):
    import numpy as np
    import rasterio
    # Open the image and read the bands as numpy arrays
    with rasterio.open(image_path) as src:
        blue = src.read(1)
        green = src.read(2)
        red = src.read(3)

    rgb = np.dstack((blue, green, red))

    return blue, green, red, rgb


def get_gsd_from_zoom(zoom_level):
    """Return pixel spacing (estimated at the equator)"""
    # See: https://wiki.openstreetmap.org/wiki/Zoom_levels
    C = 40075016.686
    return (C / 2**(zoom_level + 8))


def get_kernel_size(gsd):
    # Diameter of a object you want to convolve with
    diameter_of_object = 2  # meters

    # Number of pixels the object will cover
    diameter_of_object_px = diameter_of_object / gsd

    # Kernel size for morphological operations
    kernel_size = int(diameter_of_object_px)

    # Ensure kernel size is odd
    if kernel_size % 2 == 0:
        kernel_size += 1
    print(f"Kernel size: {kernel_size}")

    return kernel_size


def threshold(index, min=0.1, max=1.0):
    import numpy as np
    # Generate the vegetation mask
    vegetation_mask = np.full(index.shape, np.nan)
    vegetation_mask[(index >= min) & (index <= max)] = 1

    # Generate the non-vegetation mask
    non_vegetation_mask = np.full(index.shape, np.nan)
    non_vegetation_mask[(index < min) | (index > max)] = 1

    return vegetation_mask, non_vegetation_mask


def smoothing(mask, kernel_size):
    import cv2
    # Apply Gaussian blur to the mask
    blur = cv2.GaussianBlur(mask, (kernel_size, kernel_size), 1)
    return blur


def morphological_operations(mask, kernel_size):
    import cv2
    import numpy as np
    # Define the structuring element for morphological operations
    opening_kernel = np.ones((kernel_size, kernel_size), np.uint8)
    closing_kernel = np.ones((kernel_size, kernel_size), np.uint8)

    # Apply morphological operations to the mask
    opening = cv2.morphologyEx(mask, cv2.MORPH_OPEN, opening_kernel)
    closing = cv2.morphologyEx(opening, cv2.MORPH_CLOSE, closing_kernel)

    return closing


def get_vegetation_index(blue, green, red, index_type="VARI", normalize=True):
    import numpy as np
    np.seterr(divide="ignore", invalid="ignore")

    if index_type == "VARI":
        index = (green.astype(float) - red.astype(float)) / (
            green.astype(float) + red.astype(float) - blue.astype(float)
        )
    elif index_type == "GLI":
        index = (2 * green.astype(float) - red.astype(float) - blue.astype(float)) / (
            2 * green.astype(float) + red.astype(float) + blue.astype(float)
        )
    elif index_type == "RGRI":
        index = red.astype(float) / green.astype(float)
    else:
        raise ValueError(f"Unknown index type: {index_type}")

    return index


def process_image(image_path, zoom_level, index_min, index_max, vegetation_index):
    """
    Process a geospatial image to segment vegetation areas.

    Parameters:
    -----------
    image_path : str
        The file path to the geospatial image.
    zoom_level : int
        The zoom level of the image capture.
    index_min : float
        The minimum threshold value for the vegetation index.
    index_max : float
        The maximum threshold value for the vegetation index.
    vegetation_index : str
        The vegetation index to use for segmentation (e.g., 'VARI', 'GLI', 'RGRI').

    Returns:
    --------
    vegetation_mask : np.ndarray
        A binary mask representing the vegetation areas in the image.
    """

    # Set default values if parameters are None
    if index_min is None:
        index_min = 0.1  # Default VARI minimum threshold
    if vegetation_index is None:
        vegetation_index = "VARI"

    print(f"Processing image: {image_path}")

    # Get the bands of the image
    blue, green, red, rgb = get_bands(image_path)

    # Get GSD from zoom level
    gsd = get_gsd_from_zoom(zoom_level)

    # Get the kernel size for morphological operations
    kernel_size = get_kernel_size(gsd)

    # Calculate vegetation index
    veg_index = get_vegetation_index(blue, green, red, vegetation_index)

    # Generate the vegetation and non-vegetation masks
    vegetation_mask, _ = threshold(veg_index, index_min)

    # Apply morphological operations to the smoothed mask
    filtered_mask = morphological_operations(vegetation_mask, kernel_size)

    # Apply smoothing and morphological operations to the vegetation mask
    smoothed_mask = smoothing(filtered_mask, kernel_size)

    return smoothed_mask
