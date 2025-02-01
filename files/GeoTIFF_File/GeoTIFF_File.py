@fused.udf
def udf(bbox: fused.types.TileGDF, path: str, *, chip_len=256):
    import numpy as np

    utils = fused.load("https://github.com/fusedio/udfs/tree/e1fefb7/public/common/").utils
    try:
        arr, metadata = utils.read_tiff(bbox, path, output_shape=(chip_len, chip_len), return_colormap=True)
        colormap = metadata['colormap']
        colored_array = (
            np.array([colormap[value] for value in arr.flat], dtype=np.uint8)
            .reshape(arr.shape + (4,))
            .transpose(2, 0, 1)
        )
        return colored_array
    except:
        return utils.read_tiff(bbox, path, output_shape=(chip_len, chip_len))
