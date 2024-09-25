@fused.udf
def udf(
    bbox: fused.types.TileGDF = None,
    year: int = 2022,
    crop_type: str = "",
    chip_len: int = 256,
    colored: bool = True
):
    """"""
    import numpy as np

    # Helper Functions
    from utils import crop_counts, filter_crops, read_tiff

    input_tiff_path = f"s3://fused-asset/data/cdls/{year}_30m_cdls.tif"
    array_int, color_map = read_tiff(
        bbox,
        input_tiff_path,
        output_shape=(chip_len, chip_len),
        return_colormap=True
    )
    
    if crop_type:
        array_int = filter_crops(array_int, crop_type, verbose=False)

    # Print out the top 20 classes
    print(crop_counts(array_int).head(20))

    colored_array = (
        np.array([color_map[value] for value in array_int.flat], dtype=np.uint8)
        .reshape(array_int.shape + (4,))
        .transpose(2, 0, 1)
    )
    
    if colored:
        return colored_array
    else:
        return array_int
    