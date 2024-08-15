@fused.udf
def udf(bbox: fused.types.TileGDF, path: str, *, chip_len=256):
    import numpy as np

    utils = fused.load(
        "https://github.com/fusedio/udfs/tree/f928ee1/public/common/"
    ).utils

    arr, color_map = utils.read_tiff(
        bbox, path, output_shape=(chip_len, chip_len), return_colormap=True
    )

    colored_array = (
        np.array([color_map[value] for value in arr.flat], dtype=np.uint8)
        .reshape(arr.shape + (4,))
        .transpose(2, 0, 1)
    )
    return colored_array
