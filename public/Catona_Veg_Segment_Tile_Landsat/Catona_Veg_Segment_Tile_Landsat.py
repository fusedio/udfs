@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    index_min: float = 0.1,
    index_max: float = 0.6,
    index_method: int = 1,
    return_mask: bool = True,
):
    import numpy as np
    
    from utils import threshold, morphological_operations, smoothing, get_gsd_from_zoom, get_kernel_size

    collection="landsat-c2-l2"
    time_of_interest="2022-09-01/2023-10-30"
    cloud_threshold=10
    red_band="red"
    nir_band="nir08"

    import odc.stac
    import palettable
    import pystac_client
    from pystac.extensions.eo import EOExtension as eo

    # Load utility functions.
    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/5cfb808/public/common/"
    ).utils

    catalog = pystac_client.Client.open("https://earth-search.aws.element84.com/v1")
    
    # Search for imagery within a specified bounding box and time period
    items = catalog.search(
        collections=[collection],
        bbox=bbox.total_bounds,
        datetime=time_of_interest,
        query={"eo:cloud_cover": {"lt": cloud_threshold}},
    ).item_collection()
    print(f"Returned {len(items)} Items")

    # Determine the pixel spacing for the zoom level
    pixel_spacing = int(5 * 2 ** (15 - bbox.z[0]))
    print(f"{pixel_spacing = }")

    # Load imagery into an XArray dataset
    odc.stac.configure_s3_access(requester_pays=True)
    ds = odc.stac.load(
        items,
        crs="EPSG:3857",
        bands=[nir_band, red_band],
        resolution=pixel_spacing,
        bbox=bbox.total_bounds,
    ).astype(float)

    # Calculate the Normalized Difference Vegetation Index
    ndvi = (ds[nir_band] - ds[red_band]) / (ds[nir_band] + ds[red_band])

    # Select the maximum value across all times
    arr = ndvi.max(dim="time")
    veg_index = arr.values
    print(veg_index)
    # return arr
    
    ###########################################

    # Generate the vegetation and non-vegetation masks
    vegetation_mask, _ = threshold(veg_index, min=0.35, max=1)
    print(vegetation_mask.astype('uint8'))
    return vegetation_mask.astype('uint8')


    # Apply morphological operations to the smoothed mask
    # Get GSD from zoom level
    x, y, z = bbox[["x", "y", "z"]].iloc[0]
    gsd = get_gsd_from_zoom(zoom_level=z)
    # Get the kernel size for morphological operations
    kernel_size = get_kernel_size(gsd)
    filtered_mask = morphological_operations(vegetation_mask, kernel_size)
    # print(filtered_mask)

    # Apply smoothing and morphological operations to the vegetation mask
    smoothed_mask = smoothing(filtered_mask, kernel_size)

    mask = (vegetation_mask * 255).astype("uint8")

    out = np.stack([mask * 0, mask * 1, mask * 0, mask // 2])
    # print(out)

    # Calculate the area per pixel in square kilometers
    pixel_area_km2 = (out.shape[0] * out.shape[1]) / 1e6
    
    return out

