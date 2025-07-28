## ToDo:
# To make this easy you can create Fused transitional format
# def reproject_bounds_transform

def transform_to_gdf(transform, shape, crs):
    import rasterio
    common = fused.load(
    "https://github.com/fusedio/udfs/tree/fd32f9c/public/common/"
    ).utils
    bounds = rasterio.transform.array_bounds(shape[-2], shape[-1], transform)
    return common.bounds_to_gdf(bounds, crs=crs)

def reproject_bounds_shape(arr, src_crs, src_bounds, dst_crs, dst_bounds, dst_shape=(256, 256)):
    import rasterio
    from rasterio.warp import reproject, Resampling
    import numpy as np
    if not dst_shape:
        dst_shape = arr.shape
        print(f'{dst_shape=}')
    if len(dst_shape)!=len(arr.shape):
        print(f'Note: Some dimensions might drop {arr.shape=}=!{dst_shape=}')
    src_transform = rasterio.transform.from_bounds(*src_bounds, arr.shape[-1], arr.shape[-2])
    dst_transform = rasterio.transform.from_bounds(*dst_bounds, dst_shape[-1], dst_shape[-2])
    destination_data = np.zeros(dst_shape, arr.dtype)
    reproject( arr, destination_data, src_transform=src_transform, src_crs=src_crs, dst_transform=dst_transform, dst_crs=dst_crs,
                resampling=Resampling.nearest,)
    return destination_data

def reproject_transform_crs(arr, src_crs, src_transform, dst_crs, dst_transform, dst_shape=(256, 256)):
    """`dst_shape` does not effect the pixel resolution but rather the padding or cropping of data"""
    import rasterio
    from rasterio.warp import reproject, Resampling
    import numpy as np
    if not dst_shape:
        dst_shape = arr.shape
        print(f'{dst_shape=}')
    if len(dst_shape)!=len(arr.shape):
        print(f'Note: Some dimensions might drop {arr.shape=}=!{dst_shape=}')
    destination_data = np.zeros(dst_shape, arr.dtype)
    reproject( arr, destination_data, src_transform=src_transform, src_crs=src_crs, 
                                      dst_transform=dst_transform, dst_crs=dst_crs,
                                      resampling=Resampling.nearest,)
    return destination_data


def reproject_crs_shape(arr, src_crs, src_transform, dst_crs, dst_shape=(256, 256)):
    """WIP"""
    import rasterio
    from rasterio.warp import reproject, Resampling
    import numpy as np
    if not dst_shape:
        dst_shape = arr.shape
        print(f'{dst_shape=}')
    if len(dst_shape)!=len(arr.shape):
        print(f'Note: Some dimensions might drop {arr.shape=}=!{dst_shape=}')
    src_bbox = transform_to_gdf(src_transform, arr.shape, crs=src_crs)       
    dst_bbox = src_bbox.to_crs(dst_crs)   
    # TODO: (calc the extend of the data)
    # 1. find the smallest bbox inside the rotated bbox (minimum_rotated_rectangle?)
    # 2. using bbox_to_xy_slice only return the slice of the data
    # 3. need to calcualte the parent dst_shape since we are cutting after to dst_shape
    ## no need for this --v
    # from rasterio.warp import transform_bounds    
    # warped_bounds = transform_bounds(src_bbox.crs, 4326, *src_bbox.total_bounds)
    return reproject_bounds_shape(arr, src_crs, src_bbox.total_bounds, dst_crs, dst_bbox.total_bounds, dst_shape)
