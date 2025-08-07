@fused.udf
def udf(bounds: fused.types.Bounds, path: str, *, chip_len=256):
    import numpy as np
    common = fused.load('https://github.com/fusedio/udfs/tree/36f4e97/public/common/').utils
    tile = common.get_tiles(bounds, target_num_tiles = 4, clip=True)
    
    try:
        arr, metadata = common.read_tiff(tile, path, output_shape=(chip_len, chip_len), return_colormap=True, resampling='nearest')
        colormap = metadata['colormap']
        colored_array = np.array([colormap[value] for value in arr.flat], dtype=np.uint8).reshape(arr.shape + (4,)).transpose(2, 0, 1)
        return colored_array, bounds
    except:
        return common.read_tiff(tile, path, output_shape=(chip_len, chip_len)), bounds
